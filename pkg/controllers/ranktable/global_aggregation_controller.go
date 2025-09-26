/*
Copyright 2025 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package ranktable

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"time"

	clusterv1alpha1 "github.com/karmada-io/karmada/pkg/apis/cluster/v1alpha1"
	karmadautil "github.com/karmada-io/karmada/pkg/util"
	"github.com/karmada-io/karmada/pkg/util/fedinformer"
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"golang.org/x/time/rate"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
	volcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	trainingclientset "volcano.sh/apis/pkg/client/clientset/versioned/typed/training/v1alpha1"
	"volcano.sh/volcano-global/pkg/controllers/scheme"
)

const (
	ReconcilerName                 = "ranktable-reconciler"
	JobNameLabelKey                = "volcano.sh/job-name"
	JobNamespaceLabelKey           = "volcano.sh/job-namespace"
	MountJobStartHcclName          = "jobstart_hccl.json"
	MountNetworkLinksName          = "network_links.json"
	RanktableStatusInitializing    = "initializing"
	RanktableStatusCompleted       = "completed"
	RanktableVersion               = "1.4"
	GlobalRanktableSuffix          = "global-ranktable"
	NetworkLinksStatusInitializing = "initializing"
	NetworkLinksStatusCompleted    = "completed"
	GlobalNetworkLinksSuffix       = "global-network-links"
)

const (
	workQueueBaseDelay = 10 * time.Millisecond
	workQueueMaxDelay  = 10 * time.Second
	workQueueRateLimit = 50
	workQueueBurst     = 500
)

func init() {
	scheme.ReconcilerInitializers[ReconcilerName] = InitGlobalRanktableReconciler
}

type GlobalAggregationController struct {
	client.Client
	KubeClient                   *kubernetes.Clientset
	JobClient                    *volcanoclientset.Clientset
	HyperJobClient               *trainingclientset.TrainingV1alpha1Client
	ClusterPredicateFunc         predicate.Predicate
	ClusterInformerManager       genericmanager.MultiClusterInformerManager
	ClusterClientSetFunc         karmadautil.NewClusterClientSetFunc
	ClusterDynamicClientSetFunc  karmadautil.NewClusterDynamicClientSetFunc
	ClusterClientOption          karmadautil.ClientOption
	ClusterStatusUpdateFrequency metav1.Duration
	ClusterCacheSyncTimeout      metav1.Duration
	ClusterEventHandlerStore     cache.ThreadSafeStore
	Queue                        workqueue.TypedRateLimitingInterface[SyncEvent]
}

func InitGlobalRanktableReconciler(mgr controllerruntime.Manager) error {
	predicateFunc := func(obj client.Object) bool {
		cluster, ok := obj.(*clusterv1alpha1.Cluster)
		if !ok || cluster.Spec.SecretRef == nil {
			return false
		}
		return cluster.Spec.SyncMode == clusterv1alpha1.Push
	}
	clusterPredicateFunc := predicate.Funcs{
		CreateFunc: func(createEvent event.CreateEvent) bool {
			return predicateFunc(createEvent.Object)
		},
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			return predicateFunc(updateEvent.ObjectNew)
		},
		DeleteFunc: func(deleteEvent event.DeleteEvent) bool {
			return predicateFunc(deleteEvent.Object)
		},
		GenericFunc: func(e event.TypedGenericEvent[client.Object]) bool {
			return false
		},
	}

	reconciler := &GlobalAggregationController{
		Client:                       mgr.GetClient(),
		KubeClient:                   kubernetes.NewForConfigOrDie(mgr.GetConfig()),
		JobClient:                    volcanoclientset.NewForConfigOrDie(mgr.GetConfig()),
		HyperJobClient:               trainingclientset.NewForConfigOrDie(mgr.GetConfig()),
		ClusterPredicateFunc:         clusterPredicateFunc,
		ClusterInformerManager:       genericmanager.GetInstance(),
		ClusterClientSetFunc:         karmadautil.NewClusterClientSet,
		ClusterDynamicClientSetFunc:  karmadautil.NewClusterDynamicClientSet,
		ClusterClientOption:          karmadautil.ClientOption{},
		ClusterStatusUpdateFrequency: metav1.Duration{Duration: 10 * time.Second},
		ClusterCacheSyncTimeout:      metav1.Duration{Duration: 10 * time.Second},
		ClusterEventHandlerStore:     cache.NewThreadSafeStore(cache.Indexers{}, cache.Indices{}),
		Queue: workqueue.NewTypedRateLimitingQueue[SyncEvent](workqueue.NewTypedMaxOfRateLimiter(
			workqueue.NewTypedItemExponentialFailureRateLimiter[SyncEvent](workQueueBaseDelay, workQueueMaxDelay),
			&workqueue.TypedBucketRateLimiter[SyncEvent]{Limiter: rate.NewLimiter(rate.Limit(workQueueRateLimit), workQueueBurst)})),
	}
	return reconciler.SetupWithManager(mgr)
}

func (g *GlobalAggregationController) SetupWithManager(mgr controllerruntime.Manager) error {
	if err := mgr.Add(g); err != nil {
		return fmt.Errorf("failed to add GlobalAggregationController as a runnable to the manager: %w", err)
	}
	return controllerruntime.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.Cluster{}, builder.WithPredicates(g.ClusterPredicateFunc)).
		Complete(g)
}

func (g *GlobalAggregationController) Start(ctx context.Context) error {
	klog.V(4).Infof("Starting global event processor")
	wait.Until(g.RunProcessor, 0, ctx.Done())
	klog.V(4).Infof("Shutting down global event processor")
	return nil
}

func (g *GlobalAggregationController) Reconcile(ctx context.Context, request controllerruntime.Request) (controllerruntime.Result, error) {
	log := controllerruntime.LoggerFrom(ctx)
	cluster := &clusterv1alpha1.Cluster{}
	if err := g.Client.Get(ctx, request.NamespacedName, cluster); err != nil {
		// The cluster may no longer exist, in which case we stop its informer and delete its handler.
		if apierrors.IsNotFound(err) {
			log.V(4).Info("Failed to find the cluster, stop tracking it", "cluster", request.Name)
			g.ClusterEventHandlerStore.Delete(request.Name)
			g.ClusterInformerManager.Stop(request.Name)
			return controllerruntime.Result{}, nil
		}
		log.Error(err, "Failed to get the cluster", "cluster", request.Name)
		return controllerruntime.Result{}, err
	}
	err := g.syncClusterInformer(ctx, cluster)
	if err != nil {
		log.Error(err, "Failed to sync the cluster informer status", "cluster", request.Name)
		return controllerruntime.Result{}, err
	}
	return controllerruntime.Result{RequeueAfter: g.ClusterStatusUpdateFrequency.Duration}, nil
}

func (g *GlobalAggregationController) syncClusterInformer(ctx context.Context, cluster *clusterv1alpha1.Cluster) error {
	log := controllerruntime.LoggerFrom(ctx)
	log.V(4).Info("Begin to sync the cluster informer", "cluster", cluster.Name)
	defer log.V(4).Info("Finish syncing the cluster informer", "cluster", cluster.Name)

	// Get or build the informer for the given cluster
	singleClusterInformerManager := g.ClusterInformerManager.GetSingleClusterManager(cluster.Name)
	if singleClusterInformerManager == nil {
		dynamicClient, err := g.ClusterDynamicClientSetFunc(cluster.Name, g.Client, &g.ClusterClientOption)
		if err != nil {
			log.Error(err, "Failed to create the dynamic client for the cluster", "cluster", cluster.Name)
			return err
		}
		singleClusterInformerManager = g.ClusterInformerManager.ForCluster(cluster.Name, dynamicClient.DynamicClientSet, 0)
		clusterEventHandler := &ClusterEventHandler{
			clusterName:            cluster.Name,
			clusterInformerManager: singleClusterInformerManager,
			globalController:       g,
			ranktableStore:         cache.NewThreadSafeStore(cache.Indexers{}, cache.Indices{}),
		}
		singleClusterInformerManager.ForResource(ConfigMapGroupVersionResource,
			fedinformer.NewHandlerOnEvents(clusterEventHandler.OnAdd, clusterEventHandler.OnUpdate, clusterEventHandler.OnDelete))
		g.ClusterEventHandlerStore.Add(cluster.Name, clusterEventHandler)
		singleClusterInformerManager.Start()
	}

	// Sync the configmap resource for the given cluster
	if singleClusterInformerManager.IsInformerSynced(ConfigMapGroupVersionResource) {
		return nil
	}
	if err := func() error {
		synced := singleClusterInformerManager.WaitForCacheSyncWithTimeout(g.ClusterCacheSyncTimeout.Duration)
		if synced == nil {
			return fmt.Errorf("no informer factory exists for the cluster")
		}
		if !synced[ConfigMapGroupVersionResource] {
			return fmt.Errorf("syncing configmap informer timed out")
		}
		return nil
	}(); err != nil {
		log.Error(err, "Failed to sync the cluster informer", "cluster", cluster.Name)
		g.ClusterEventHandlerStore.Delete(cluster.Name)
		singleClusterInformerManager.Stop()
		return err
	}
	log.V(4).Info("Successful to sync the cluster informer", "cluster", cluster.Name)
	return nil
}

func (g *GlobalAggregationController) RunProcessor() {
	for g.processNextItem() {
	}
}

func (g *GlobalAggregationController) processNextItem() bool {
	syncEvent, quit := g.Queue.Get()
	if quit {
		return false
	}
	defer g.Queue.Done(syncEvent)

	klog.V(4).Infof("Begin to handle sync event %s", syncEvent.getKey())
	defer klog.V(4).Infof("Finishing handling sync event %s", syncEvent.getKey())

	if err := g.handleEvent(syncEvent); err != nil {
		g.Queue.AddRateLimited(syncEvent)
		klog.V(4).Infof("Failed to handle sync event %s, err: %v", syncEvent.getKey(), err)
	} else {
		g.Queue.Forget(syncEvent)
	}
	return true
}

func (g *GlobalAggregationController) handleEvent(syncEvent SyncEvent) error {
	// obtain the corresponding hyperJob
	ctx := context.TODO()
	job, err := g.JobClient.BatchV1alpha1().Jobs(syncEvent.Namespace).Get(ctx, syncEvent.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	jobOwner, err := getJobOwner(job)
	if err != nil {
		return err
	}
	hyperJob, err := g.HyperJobClient.HyperJobs(job.Namespace).Get(ctx, jobOwner.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if hyperJob.DeletionTimestamp != nil {
		klog.V(4).Infof("hyperJob %s has been deleted, no need to sync", hyperJob.Name)
		return nil
	}

	// update the global ranktable for the hyperJob
	globalRanktable := g.generateHyperJobGlobalRanktable(hyperJob)
	if globalRanktable.Status != RanktableStatusCompleted {
		klog.V(4).Infof("The global ranktable of hyperJob %s is not completed, no need to update its configMap", hyperJob.Name)
		return nil
	}
	configMapName := fmt.Sprintf("%s-%s", hyperJob.Name, GlobalRanktableSuffix)
	currentConfigMap, err := g.KubeClient.CoreV1().ConfigMaps(hyperJob.Namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	configMap := currentConfigMap.DeepCopy()
	currentGlobalRanktable, err := getGlobalRanktableFromConfigMap(currentConfigMap)
	if err != nil {
		// in this case, the configMap's current version has no ranktable, or its ranktable is invalid, and we set the DataVersion of the new global ranktable to 1
		klog.V(4).Infof("The current version of configMap %s has no valid ranktable, and we set the DataVersion of the new global ranktable to 1", configMapName)
		globalRanktable.DataVersion = 1
	} else {
		globalRanktable.DataVersion = currentGlobalRanktable.DataVersion + 1
	}
	globalRanktableBytes, err := json.Marshal(globalRanktable)
	if err != nil {
		return err
	}
	if len(configMap.Data) == 0 {
		configMap.Data = make(map[string]string)
	}
	configMap.Data[MountJobStartHcclName] = string(globalRanktableBytes)
	_, err = g.KubeClient.CoreV1().ConfigMaps(hyperJob.Namespace).Update(ctx, configMap, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update configMap %s", configMapName)
		return err
	} else {
		klog.V(4).Infof("Successful to update configMap %s", configMapName)
	}

	// update the global network links for the hyperJob
	configMapName = fmt.Sprintf("%s-%s", hyperJob.Name, GlobalNetworkLinksSuffix)
	currentConfigMap, err = g.KubeClient.CoreV1().ConfigMaps(hyperJob.Namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	configMap = currentConfigMap.DeepCopy()
	globalNetworkLinks, err := g.generateHyperJobGlobalNetworkLinks(ctx, hyperJob)
	if err != nil {
		return err
	}
	currentGlobalNetworkLinks, err := getGlobalNetworkLinksFromConfigMap(currentConfigMap)
	if err != nil {
		// in this case, the configMap's current version has no network links, or its network links are invalid, and we set the DataVersion of the new global network links to 1
		klog.V(4).Infof("The current version of configMap %s has no valid network links, and we set the DataVersion of the new global network links to 1", configMapName)
		globalNetworkLinks.DataVersion = 1
	} else {
		globalNetworkLinks.DataVersion = currentGlobalNetworkLinks.DataVersion + 1
	}
	globalNetworkLinksBytes, err := json.Marshal(globalNetworkLinks)
	if err != nil {
		return err
	}
	if len(configMap.Data) == 0 {
		configMap.Data = make(map[string]string)
	}
	configMap.Data[MountNetworkLinksName] = string(globalNetworkLinksBytes)

	_, err = g.KubeClient.CoreV1().ConfigMaps(hyperJob.Namespace).Update(ctx, configMap, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Failed to update configMap %s", configMapName)
		return err
	} else {
		klog.V(4).Infof("Successful to update configMap %s", configMapName)
	}
	return nil
}

func (g *GlobalAggregationController) getJobRanktableInfo(namespace string, name string, clusterNames []string) *SingleRanktableInfo {
	if len(clusterNames) == 0 {
		clusterNames = g.ClusterEventHandlerStore.ListKeys()
	}
	for _, clusterName := range clusterNames {
		if clusterEventHandlerObj, exists := g.ClusterEventHandlerStore.Get(clusterName); exists {
			clusterEventHandler, ok := clusterEventHandlerObj.(*ClusterEventHandler)
			if !ok {
				continue
			}
			if ranktableObj, exists := clusterEventHandler.ranktableStore.Get(getSingleRanktableKeyByJobNamespaceAndName(namespace, name)); exists {
				ranktable, ok := ranktableObj.(*SingleRanktable)
				if !ok {
					continue
				}
				return &SingleRanktableInfo{clusterId: clusterName, jobName: name, ranktable: ranktable}
			}
		}
	}
	return nil
}

func (g *GlobalAggregationController) generateHyperJobGlobalRanktable(hyperJob *trainingv1alpha1.HyperJob) *GlobalRanktable {
	ranktableInfoSlice := make([]*SingleRanktableInfo, 0)
	allCompleted := true
	// get all the existing ranktables belonging to this hyperJob
	for _, replicatedJob := range hyperJob.Spec.ReplicatedJobs {
		for i := 0; i < int(replicatedJob.Replicas); i++ {
			jobName := fmt.Sprintf("%s-%s-%d", hyperJob.Name, replicatedJob.Name, i)
			ranktableInfo := g.getJobRanktableInfo(hyperJob.Namespace, jobName, replicatedJob.ClusterNames)
			if ranktableInfo == nil {
				allCompleted = false
			} else {
				ranktableInfoSlice = append(ranktableInfoSlice, ranktableInfo)
				if ranktableInfo.ranktable.Status != RanktableStatusCompleted {
					allCompleted = false
				}
			}
		}
	}
	// sort the ranktables in increasing order of their clusterIds and jobNames
	sort.Slice(ranktableInfoSlice, func(i, j int) bool {
		if ranktableInfoSlice[i].clusterId < ranktableInfoSlice[j].clusterId {
			return true
		}
		if ranktableInfoSlice[i].clusterId == ranktableInfoSlice[j].clusterId && ranktableInfoSlice[i].jobName <= ranktableInfoSlice[j].jobName {
			return true
		}
		return false
	})
	// aggregate the ranktables into one global ranktable, arrange the rankIds and the clusterList
	globalRanktable := NewGlobalRanktable()
	rankId := 0
	var clusterBase ClusterBase
	for _, ranktableInfo := range ranktableInfoSlice {
		for i := range ranktableInfo.ranktable.ServerList {
			for j := range ranktableInfo.ranktable.ServerList[i].Device {
				ranktableInfo.ranktable.ServerList[i].Device[j].RankId = strconv.Itoa(rankId)
				rankId++
			}
		}
		globalRanktable.ServerList = append(globalRanktable.ServerList, ranktableInfo.ranktable.ServerList...)
		globalRanktable.SuperPodList = append(globalRanktable.SuperPodList, ranktableInfo.ranktable.SuperPodList...)

		if clusterBase.ClusterId != ranktableInfo.clusterId {
			if clusterBase.ClusterId != "" {
				globalRanktable.ClusterList = append(globalRanktable.ClusterList, clusterBase)
			}
			clusterBase = ClusterBase{ClusterId: ranktableInfo.clusterId}
			if len(ranktableInfo.ranktable.ClusterList) > 0 {
				clusterBase.AZId = ranktableInfo.ranktable.ClusterList[0].AZId
				clusterBase.RegionId = ranktableInfo.ranktable.ClusterList[0].RegionId
			}
			clusterBase.SuperPodList = make([]SuperPodBase, 0)
		}
		for _, superPod := range ranktableInfo.ranktable.SuperPodList {
			clusterBase.SuperPodList = append(clusterBase.SuperPodList, SuperPodBase{SuperPodId: superPod.SuperPodId})
		}
	}
	if clusterBase.ClusterId != "" {
		globalRanktable.ClusterList = append(globalRanktable.ClusterList, clusterBase)
	}
	globalRanktable.ServerCount = strconv.Itoa(len(globalRanktable.ServerList))
	if allCompleted {
		globalRanktable.Status = RanktableStatusCompleted
	} else {
		globalRanktable.Status = RanktableStatusInitializing
	}
	return globalRanktable
}

func (g *GlobalAggregationController) getJobNetworkLinksInfo(ctx context.Context, namespace string, name string, clusterNames []string) (map[string]string, error) {
	if len(clusterNames) == 0 {
		clusterNames = g.ClusterEventHandlerStore.ListKeys()
	}
	labelMap := map[string]string{JobNamespaceLabelKey: namespace, JobNameLabelKey: name}
	labelSelector := labels.SelectorFromSet(labelMap)
	for _, clusterName := range clusterNames {
		if clusterEventHandlerObj, exists := g.ClusterEventHandlerStore.Get(clusterName); exists {
			clusterEventHandler, ok := clusterEventHandlerObj.(*ClusterEventHandler)
			if !ok {
				continue
			}
			if _, exists = clusterEventHandler.ranktableStore.Get(getSingleRanktableKeyByJobNamespaceAndName(namespace, name)); exists {
				dynamicClient, err := g.ClusterDynamicClientSetFunc(clusterName, g.Client, &g.ClusterClientOption)
				if err != nil {
					klog.V(4).Infof("Failed to get the dynamic client of cluster %s for job %s, err: %v", clusterName, name, err)
					return nil, err
				}
				unstructuredList, err := dynamicClient.DynamicClientSet.Resource(PodGroupVersionResource).List(ctx, metav1.ListOptions{LabelSelector: labelSelector.String()})
				if err != nil {
					klog.V(4).Infof("Failed to get the selected pod list of cluster %s for job %s, err: %v", clusterName, name, err)
					return nil, err
				}
				networkLinkMap := make(map[string]string)
				for _, item := range unstructuredList.Items {
					pod := &corev1.Pod{}
					if err = runtime.DefaultUnstructuredConverter.FromUnstructured(item.Object, pod); err != nil {
						klog.V(4).Infof("Failed to convert the unstructured item of cluster %s for job %s to a pod, err: %v", clusterName, name, err)
						return nil, err
					} else {
						networkLinkMap[fmt.Sprintf("%s.%s", pod.Name, clusterName)] = pod.Status.PodIP
					}
				}
				return networkLinkMap, nil
			}
		}
	}
	return nil, fmt.Errorf("no network links are found for job %s", name)
}
func (g *GlobalAggregationController) generateHyperJobGlobalNetworkLinks(ctx context.Context, hyperJob *trainingv1alpha1.HyperJob) (*GlobalNetworkLinks, error) {
	globalNetworkLinks := NewGlobalNetworkLinks()
	allCompleted := true
	for _, replicatedJob := range hyperJob.Spec.ReplicatedJobs {
		for i := 0; i < int(replicatedJob.Replicas); i++ {
			jobName := fmt.Sprintf("%s-%s-%d", hyperJob.Name, replicatedJob.Name, i)
			networkLinksMap, err := g.getJobNetworkLinksInfo(ctx, hyperJob.Namespace, jobName, replicatedJob.ClusterNames)
			if err != nil {
				return nil, err
			}
			totalPodNumber := 0
			for _, task := range replicatedJob.TemplateSpec.Tasks {
				totalPodNumber += int(task.Replicas)
			}
			if len(networkLinksMap) != totalPodNumber {
				klog.V(4).Infof("Failed to get all the network links of job %s", jobName)
				allCompleted = false
			}
			for key, value := range networkLinksMap {
				globalNetworkLinks.NetworkLinks[key] = value
			}
		}
	}
	if allCompleted {
		globalNetworkLinks.Status = NetworkLinksStatusCompleted
	} else {
		globalNetworkLinks.Status = NetworkLinksStatusInitializing
	}
	globalNetworkLinks.PodCount = strconv.Itoa(len(globalNetworkLinks.NetworkLinks))
	return globalNetworkLinks, nil
}
