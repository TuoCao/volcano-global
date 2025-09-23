package ranktable

import (
	"context"

	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	controllerruntime "sigs.k8s.io/controller-runtime"
)

type ClusterEventHandler struct {
	clusterName            string
	clusterInformerManager genericmanager.SingleClusterInformerManager
	globalController       *GlobalAggregationController
	ranktableStore         cache.ThreadSafeStore
}

func (c *ClusterEventHandler) EventFilter(obj interface{}) bool {
	configMap := convertObjToConfigMap(obj)
	if configMap == nil {
		return false
	}
	owner := c.getConfigMapOwner(configMap)
	return owner != nil
}

func (c *ClusterEventHandler) OnAdd(obj interface{}) {
	ctx := context.TODO()
	log := controllerruntime.LoggerFrom(ctx)
	log.V(4).Info("Start to handle an add/update event")
	defer log.V(4).Info("Finish handling the add/update event")
	configMap := convertObjToConfigMap(obj)
	owner := c.getConfigMapOwner(configMap)
	ranktable := getSingleRanktableFromConfigmap(configMap)
	if ranktable == nil {
		log.V(4).Info("The configMap has no ranktable", "configMap", configMap.Name)
		return
	}
	ranktableKey := getSingleRanktableKeyByNamespaceAndName(configMap.Namespace, owner.Name)
	if currentRanktableObj, exists := c.ranktableStore.Get(ranktableKey); exists {
		currentRanktable, _ := currentRanktableObj.(*SingleRanktable)
		if ranktable.DataVersion <= currentRanktable.DataVersion {
			log.V(4).Info("The ranktable is outdated, no need to update",
				"ranktableDataVersion", ranktable.DataVersion, "currentRanktableDataVersion", currentRanktable.DataVersion)
			return
		} else {
			c.ranktableStore.Update(ranktableKey, ranktable)
			log.V(4).Info("The ranktable has been successfully updated")
		}
	} else {
		c.ranktableStore.Add(ranktableKey, ranktable)
		log.V(4).Info("The ranktable has been successfully added")
	}
	c.globalController.GlobalEventChan <- EventKeyInfo{Namespace: configMap.Namespace, Name: owner.Name}
}

func (c *ClusterEventHandler) OnUpdate(_, newObj interface{}) {
	c.OnAdd(newObj)
}

func (c *ClusterEventHandler) OnDelete(obj interface{}) {
	ctx := context.TODO()
	log := controllerruntime.LoggerFrom(ctx)
	log.V(4).Info("Start to handle a delete event")
	defer log.V(4).Info("Finish handling the delete event")
	configMap := convertObjToConfigMap(obj)
	owner := c.getConfigMapOwner(configMap)
	ranktable := getSingleRanktableFromConfigmap(configMap)
	if ranktable == nil {
		log.V(4).Info("The configMap has no ranktable", "configMap", configMap.Name)
		return
	}
	ranktableKey := getSingleRanktableKeyByNamespaceAndName(configMap.Namespace, owner.Name)
	if currentRanktableObj, exists := c.ranktableStore.Get(ranktableKey); exists {
		currentRanktable, _ := currentRanktableObj.(*SingleRanktable)
		if ranktable.DataVersion < currentRanktable.DataVersion {
			log.V(4).Info("The ranktable is outdated, no need to update",
				"ranktableDataVersion", ranktable.DataVersion, "currentRanktableDataVersion", currentRanktable.DataVersion)
		} else {
			c.ranktableStore.Delete(ranktableKey)
			log.V(4).Info("The ranktable has been successfully deleted")
			c.globalController.GlobalEventChan <- EventKeyInfo{Namespace: configMap.Namespace, Name: owner.Name}
		}
	} else {
		log.V(4).Info("The ranktable has not been stored before, no need to delete", "configMap", configMap.Name)
	}
}

func (c *ClusterEventHandler) getConfigMapOwner(configMap *corev1.ConfigMap) *metav1.OwnerReference {
	for _, owner := range configMap.OwnerReferences {
		ownerGV, _ := schema.ParseGroupVersion(owner.APIVersion)
		ownerGVK := schema.GroupVersionKind{
			Group:   ownerGV.Group,
			Version: ownerGV.Version,
			Kind:    owner.Kind,
		}
		if ownerGVK == JobGroupVersionKind {
			return &owner
		}
	}
	return nil
}
