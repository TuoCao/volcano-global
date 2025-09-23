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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
	volcanoclientset "volcano.sh/apis/pkg/client/clientset/versioned"
	trainingclientset "volcano.sh/apis/pkg/client/clientset/versioned/typed/training/v1alpha1"

	"volcano.sh/volcano-global/pkg/controllers/scheme"
)

const (
	ReconcilerName                 = "ranktable-reconciler"
	JobNameLabelKey                = "volcano.sh/job-name"
	JobNamespaceLableKey           = "volcano.sh/job-namespace"
	MountJobStartHcclName          = "jobstart_hccl.json"
	MountRanktableTorName          = "ranktable_tor.json"
	MountNetworkLinksName          = "network_links.json"
	RanktableStatusInitializing    = "initializing"
	RanktableStatusCompleted       = "completed"
	RanktableVersion               = "1.4"
	GlobalRanktableSuffix          = "global-ranktable"
	NetworkLinksStatusInitializing = "initializing"
	NetworkLinksStatusCompleted    = "completed"
	GlobalNetworkLinksSuffix       = "global-network-links"
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
	GlobalEventChan              chan EventKeyInfo
}

func InitGlobalRanktableReconciler(mgr controllerruntime.Manager) error {
	clusterPredicateFunc := predicate.Funcs{
		CreateFunc: func(createEvent event.CreateEvent) bool {
			obj := createEvent.Object.(*clusterv1alpha1.Cluster)
			if obj.Spec.SecretRef == nil {
				return false
			}
			return obj.Spec.SyncMode == clusterv1alpha1.Push
		},
		UpdateFunc: func(updateEvent event.UpdateEvent) bool {
			obj := updateEvent.ObjectNew.(*clusterv1alpha1.Cluster)
			if obj.Spec.SecretRef == nil {
				return false
			}
			return obj.Spec.SyncMode == clusterv1alpha1.Push
		},
		DeleteFunc: func(deleteEvent event.DeleteEvent) bool {
			obj := deleteEvent.Object.(*clusterv1alpha1.Cluster)
			if obj.Spec.SecretRef == nil {
				return false
			}
			return obj.Spec.SyncMode == clusterv1alpha1.Push
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
		GlobalEventChan:              make(chan EventKeyInfo, 100),
	}
	go reconciler.processGlobalEvents(context.TODO())
	return reconciler.SetupWithManager(mgr)
}

func (g *GlobalAggregationController) SetupWithManager(mgr controllerruntime.Manager) error {
	return controllerruntime.NewControllerManagedBy(mgr).
		For(&clusterv1alpha1.Cluster{}, builder.WithPredicates(g.ClusterPredicateFunc)).
		Complete(g)
}

func (g *GlobalAggregationController) Reconcile(ctx context.Context, request controllerruntime.Request) (controllerruntime.Result, error) {
	log := controllerruntime.LoggerFrom(ctx)
	cluster := &clusterv1alpha1.Cluster{}
	if err := g.Client.Get(ctx, request.NamespacedName, cluster); err != nil {
		// The cluster may no longer exist, in which case we stop its informer and delete its handler.
		// todo:
		if apierrors.IsNotFound(err) {
			log.V(4).Info("Failed to find the cluster, stop tracking it", "cluster", request.Name)
			g.ClusterEventHandlerStore.Delete(request.Name)
			g.ClusterInformerManager.Stop(request.Name)
			return controllerruntime.Result{}, nil
		}
		log.Error(err, "Failed to get the cluster, stop tracking it", "cluster", request.Name)
		return controllerruntime.Result{}, err
	}
	err := g.syncClusterInformer(ctx, cluster)
	if err != nil {
		log.Error(err, "Failed to sync the cluster informer status", "cluster", request.Name)
	}
	return controllerruntime.Result{RequeueAfter: g.ClusterStatusUpdateFrequency.Duration}, err
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
			fedinformer.NewFilteringHandlerOnAllEvents(clusterEventHandler.EventFilter, clusterEventHandler.OnAdd, clusterEventHandler.OnUpdate, clusterEventHandler.OnDelete))
		singleClusterInformerManager.ForResource(PodGroupVersionResource, cache.ResourceEventHandlerFuncs{})
		g.ClusterEventHandlerStore.Add(cluster.Name, clusterEventHandler)
		singleClusterInformerManager.Start()
	}

	// Sync the configmap resource for the given cluster
	if singleClusterInformerManager.IsInformerSynced(ConfigMapGroupVersionResource) && singleClusterInformerManager.IsInformerSynced(PodGroupVersionResource) {
		return nil
	}
	if err := func() error {
		synced := singleClusterInformerManager.WaitForCacheSyncWithTimeout(g.ClusterCacheSyncTimeout.Duration)
		if synced == nil {
			return fmt.Errorf("no informer factory exists for the cluster")
		}
		if !synced[ConfigMapGroupVersionResource] || !synced[PodGroupVersionResource] {
			return fmt.Errorf("syncing configmap and pod informer timed out")
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

func (g *GlobalAggregationController) processGlobalEvents(ctx context.Context) {
	for {
		eventKeyInfo := <-g.GlobalEventChan
		g.syncGlobalRanktableAndNetworkLinks(ctx, eventKeyInfo)
	}
}

func (g *GlobalAggregationController) syncGlobalRanktableAndNetworkLinks(ctx context.Context, event EventKeyInfo) {
	log := controllerruntime.LoggerFrom(ctx)
	log.V(4).Info("Begin to sync the global ranktable and network links", "jobName", event.Name)
	defer log.V(4).Info("Finish syncing the global ranktable and network links", "jobName", event.Name)

	// obtain the corresponding hyperJob
	job, err := g.JobClient.BatchV1alpha1().Jobs(event.Namespace).Get(ctx, event.Name, metav1.GetOptions{})
	if err != nil {
		log.Error(err, "Failed to get the job", "job", event.Name)
		return
	}
	jobOwner, err := g.getJobOwner(job)
	if err != nil {
		log.Error(err, "Failed to get the owner reference of the job", "job", event.Name)
		return
	}
	hyperJob, err := g.HyperJobClient.HyperJobs(job.Namespace).Get(ctx, jobOwner.Name, metav1.GetOptions{})
	if err != nil {
		log.Error(err, "Failed to get the hyperJob", "hyperJob", jobOwner.Name)
		return
	}
	if hyperJob.DeletionTimestamp != nil {
		log.V(4).Info("The corresponding hyperJob has been deleted, no need to sync", "hyperJob", hyperJob.Name)
		return
	}

	// update the global ranktable for the hyperJob
	configMapName := fmt.Sprintf("%s-%s", hyperJob.Name, GlobalRanktableSuffix)
	currentConfigMap, err := g.KubeClient.CoreV1().ConfigMaps(hyperJob.Namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		log.Error(err, "Failed to get the configMap of the global ranktable", "configmap", configMapName)
		return
	}
	configMap := currentConfigMap.DeepCopy()
	globalRanktable := g.generateGlobalRanktableForHyperJob(hyperJob)
	currentGlobalRanktable := getGlobalRanktableFromConfigMap(currentConfigMap)
	if currentGlobalRanktable != nil {
		globalRanktable.DataVersion = currentGlobalRanktable.DataVersion + 1
	}
	globalRanktableBytes, _ := json.Marshal(globalRanktable)
	if len(configMap.Data) == 0 {
		configMap.Data = make(map[string]string)
	}
	configMap.Data[MountJobStartHcclName] = string(globalRanktableBytes)

	_, err = g.KubeClient.CoreV1().ConfigMaps(hyperJob.Namespace).Update(ctx, configMap, metav1.UpdateOptions{})
	if err != nil {
		log.Error(err, "Failed to update the configMap of the global ranktable", "configMap", configMapName)
	} else {
		log.V(4).Info("Successful to update the configMap of the global ranktable", "configMap", configMapName)
	}
	if globalRanktable.Status == RanktableStatusCompleted {
		g.syncGlobalNetworkLinks(ctx, hyperJob)
	} else {
		log.V(4).Info("The global ranktable is not completed, no need to sync the global network links", "configMap", configMapName)
	}
}

func (g *GlobalAggregationController) getJobRanktableInfo(namespace string, name string, clusterNames []string) *SingleRanktableInfo {
	if len(clusterNames) == 0 {
		clusterNames = g.ClusterEventHandlerStore.ListKeys()
	}
	for _, clusterName := range clusterNames {
		if clusterEventHandlerObj, exists := g.ClusterEventHandlerStore.Get(clusterName); exists {
			clusterEventHandler, _ := clusterEventHandlerObj.(*ClusterEventHandler)
			if ranktableObj, exists := clusterEventHandler.ranktableStore.Get(getSingleRanktableKeyByNamespaceAndName(namespace, name)); exists {
				ranktable, _ := ranktableObj.(*SingleRanktable)
				return &SingleRanktableInfo{clusterId: clusterName, jobName: name, ranktable: ranktable}
			}
		}
	}
	return nil
}

// todo: implement the workflow of consulting PSM
func (g *GlobalAggregationController) generateGlobalRanktableForHyperJob(hyperJob *trainingv1alpha1.HyperJob) *GlobalRanktable {
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
		} else if ranktableInfoSlice[i].clusterId == ranktableInfoSlice[j].clusterId && ranktableInfoSlice[i].jobName <= ranktableInfoSlice[j].jobName {
			return true
		} else {
			return false
		}
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

func (g *GlobalAggregationController) getJobOwner(job *batchv1alpha1.Job) (*metav1.OwnerReference, error) {
	for _, owner := range job.OwnerReferences {
		ownerGV, _ := schema.ParseGroupVersion(owner.APIVersion)
		ownerGVK := schema.GroupVersionKind{
			Group:   ownerGV.Group,
			Version: ownerGV.Version,
			Kind:    owner.Kind,
		}
		if ownerGVK == HyperJobGroupVersionKind {
			return &owner, nil
		}
	}
	return nil, fmt.Errorf("job %s/%s does not have a wanted owner reference", job.Namespace, job.Name)
}

func (g *GlobalAggregationController) syncGlobalNetworkLinks(ctx context.Context, hyperJob *trainingv1alpha1.HyperJob) {
	log := controllerruntime.LoggerFrom(ctx)
	log.V(4).Info("Begin to sync the global network links", "hyperJobName", hyperJob.Name)
	defer log.V(4).Info("Finish syncing the global network links", "hyperJobName", hyperJob.Name)

	// update the global network links for the hyperJob
	configMapName := fmt.Sprintf("%s-%s", hyperJob.Name, GlobalNetworkLinksSuffix)
	currentConfigMap, err := g.KubeClient.CoreV1().ConfigMaps(hyperJob.Namespace).Get(ctx, configMapName, metav1.GetOptions{})
	if err != nil {
		log.Error(err, "Failed to get the configMap of the global network links", "configmap", configMapName)
		return
	}
	configMap := currentConfigMap.DeepCopy()
	globalNetworkLinks := g.generateGlobalNetworkLinksForHyperJob(ctx, hyperJob)
	currentGlobalNetworkLinks := getGlobalNetworkLinksFromConfigMap(currentConfigMap)
	if currentGlobalNetworkLinks != nil {
		globalNetworkLinks.DataVersion = currentGlobalNetworkLinks.DataVersion + 1
	}
	globalNetworkLinksBytes, _ := json.Marshal(globalNetworkLinks)
	if len(configMap.Data) == 0 {
		configMap.Data = make(map[string]string)
	}
	configMap.Data[MountNetworkLinksName] = string(globalNetworkLinksBytes)

	_, err = g.KubeClient.CoreV1().ConfigMaps(hyperJob.Namespace).Update(ctx, configMap, metav1.UpdateOptions{})
	if err != nil {
		log.Error(err, "Failed to update the configMap of the global network links", "configMap", configMapName)
	} else {
		log.V(4).Info("Successful to update the configMap of the network links", "configMap", configMapName)
	}
}

func (g *GlobalAggregationController) getJobClusterName(namespace string, name string, clusterNames []string) string {
	if len(clusterNames) == 0 {
		clusterNames = g.ClusterEventHandlerStore.ListKeys()
	}
	for _, clusterName := range clusterNames {
		if clusterEventHandlerObj, exists := g.ClusterEventHandlerStore.Get(clusterName); exists {
			clusterEventHandler, _ := clusterEventHandlerObj.(*ClusterEventHandler)
			if _, exists = clusterEventHandler.ranktableStore.Get(getSingleRanktableKeyByNamespaceAndName(namespace, name)); exists {
				return clusterName
			}
		}
	}
	return ""
}

func (g *GlobalAggregationController) generateGlobalNetworkLinksForHyperJob(ctx context.Context, hyperJob *trainingv1alpha1.HyperJob) *GlobalNetworkLinks {
	log := controllerruntime.LoggerFrom(ctx)
	globalNetworkLinks := NewGlobalNetworkLinks()
	allCompleted := true
	for _, replicatedJob := range hyperJob.Spec.ReplicatedJobs {
		for i := 0; i < int(replicatedJob.Replicas); i++ {
			jobName := fmt.Sprintf("%s-%s-%d", hyperJob.Name, replicatedJob.Name, i)
			clusterName := g.getJobClusterName(hyperJob.Namespace, jobName, replicatedJob.ClusterNames)
			for _, task := range replicatedJob.TemplateSpec.Tasks {
				for j := 0; j < int(task.Replicas); j++ {
					podName := fmt.Sprintf("%s-%s-%d-%s-%d", hyperJob.Name, replicatedJob.Name, i, task.Name, j)
					obj, err := g.ClusterInformerManager.GetSingleClusterManager(clusterName).Lister(PodGroupVersionResource).ByNamespace(hyperJob.Namespace).Get(podName)
					if err != nil {
						allCompleted = false
						continue
					}
					pod := convertObjToPod(obj)
					if pod == nil {
						log.V(4).Info("Failed to convert the obj to the pod", "pod", podName)
						continue
					}
					globalNetworkLinks.NetworkLinks[fmt.Sprintf("%s.%s", podName, clusterName)] = pod.Status.PodIP
				}
			}
		}
	}
	if allCompleted {
		globalNetworkLinks.Status = NetworkLinksStatusCompleted
	} else {
		globalNetworkLinks.Status = NetworkLinksStatusInitializing
	}
	globalNetworkLinks.PodCount = strconv.Itoa(len(globalNetworkLinks.NetworkLinks))
	return globalNetworkLinks
}

//func (g *GlobalAggregationController) getClusterNamesOfHyperJob(hyperJob *trainingv1alpha1.HyperJob) []string {
//	clusterNameMap := make(map[string]struct{})
//	clusterNames := make([]string, 0)
//	for _, replicatedJob := range hyperJob.Spec.ReplicatedJobs {
//		if len(replicatedJob.ClusterNames) == 0 {
//			return g.ClusterEventHandlerStore.ListKeys()
//		} else {
//			for _, clusterName := range replicatedJob.ClusterNames {
//				if _, ok := clusterNameMap[clusterName]; !ok {
//					clusterNameMap[clusterName] = struct{}{}
//					clusterNames = append(clusterNames, clusterName)
//				}
//			}
//		}
//	}
//	return clusterNames
//}

//func (g *GlobalAggregationController) getJobPodIpInfo(namespace string, name string, clusterNames []string) []SingleNetworkLinkInfo {
//	if len(clusterNames) == 0 {
//		clusterNames = g.ClusterEventHandlerStore.ListKeys()
//	}
//	selector, _ := labels.Parse(fmt.Sprintf("%s=%s, %s=%s", JobNamespaceLableKey, namespace, JobNameLabelKey, name))
//	for _, clusterName := range clusterNames {
//		objs, _ := g.ClusterInformerManager.GetSingleClusterManager(clusterName).Lister(PodGroupVersionResource).List(selector)
//		if len(objs) > 0 {
//			networkLinkMap := make(map[string]string)
//			for _, obj := range objs {
//				pod, _ := obj.(*corev1.Pod)
//				networkLinkMap[fmt.Sprintf("%s.%s", pod.Name, clusterName)] = pod.Status.PodIP
//
//			}
//			return singlePodIpInfoSlince
//		}
//	}
//	return nil
//}
