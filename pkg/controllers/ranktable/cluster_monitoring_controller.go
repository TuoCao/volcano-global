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
	"github.com/karmada-io/karmada/pkg/util/fedinformer/genericmanager"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"
)

type ClusterEventHandler struct {
	clusterName            string
	clusterInformerManager genericmanager.SingleClusterInformerManager
	globalController       *GlobalAggregationController
	ranktableStore         cache.ThreadSafeStore
}

func (c *ClusterEventHandler) OnAdd(obj interface{}) {
	klog.V(4).Infof("Begin to handle an add/update event on cluster %s", c.clusterName)
	defer klog.V(4).Infof("Finish handling the add/update event on cluster %s", c.clusterName)
	configMap, err := convertObjToConfigMap(obj)
	if err != nil {
		klog.V(4).Infof("The obj of this event is not a configMap, err: %v", err)
		return
	}
	owner, err := getConfigMapOwner(configMap)
	if err != nil {
		klog.V(4).Infof("ConfigMap %s dose not have a wanted owner reference, err: %v", configMap.Name, err)
		return
	}
	ranktable, err := getSingleRanktableFromConfigMap(configMap)
	if err != nil {
		klog.V(4).Infof("Failed to get the ranktable of configMap %s, err: %v", configMap.Name, err)
		return
	}
	ranktableKey := getSingleRanktableKeyByJobNamespaceAndName(configMap.Namespace, owner.Name)
	if currentRanktableObj, exists := c.ranktableStore.Get(ranktableKey); exists {
		currentRanktable, ok := currentRanktableObj.(*SingleRanktable)
		if !ok {
			c.ranktableStore.Update(ranktableKey, ranktable)
			klog.V(4).Infof("The type of the object (key is %s) in the store is not *ranktable, overwritted it", ranktableKey)
		} else if ranktable.DataVersion <= currentRanktable.DataVersion {
			klog.V(4).Infof("The ranktable of comfigMap %s is outdated, no need to update", configMap.Name)
			return
		} else {
			c.ranktableStore.Update(ranktableKey, ranktable)
			klog.V(4).Infof("The ranktable of configMap %s has been successfully updated", configMap.Name)
		}
	} else {
		c.ranktableStore.Add(ranktableKey, ranktable)
		klog.V(4).Infof("The ranktable of configMap %s has been successfully added", configMap.Name)
	}
	c.globalController.Queue.Add(SyncEvent{Namespace: configMap.Namespace, Name: owner.Name})
}

func (c *ClusterEventHandler) OnUpdate(_, newObj interface{}) {
	c.OnAdd(newObj)
}

func (c *ClusterEventHandler) OnDelete(obj interface{}) {
	klog.V(4).Infof("Begin to handle a delete event on cluster %s", c.clusterName)
	defer klog.V(4).Infof("Finish handling the delete event on cluster %s", c.clusterName)
	configMap, err := convertObjToConfigMap(obj)
	if err != nil {
		klog.V(4).Infof("The obj of this event is not a configMap, err: %v", err)
		return
	}
	owner, err := getConfigMapOwner(configMap)
	if err != nil {
		klog.V(4).Infof("ConfigMap %s dose not have a wanted owner reference, err: %v", configMap.Name, err)
		return
	}
	ranktable, err := getSingleRanktableFromConfigMap(configMap)
	if err != nil {
		klog.V(4).Infof("Failed to get the ranktable of configMap %s, err: %v", configMap.Name, err)
		return
	}
	ranktableKey := getSingleRanktableKeyByJobNamespaceAndName(configMap.Namespace, owner.Name)
	if currentRanktableObj, exists := c.ranktableStore.Get(ranktableKey); exists {
		currentRanktable, ok := currentRanktableObj.(*SingleRanktable)
		if !ok {
			c.ranktableStore.Delete(ranktableKey)
			klog.V(4).Infof("The type of the object (key is %s) in the store is not *ranktable, deleted it", ranktableKey)
		}
		if ranktable.DataVersion < currentRanktable.DataVersion {
			klog.V(4).Infof("The ranktable of comfigMap %s is outdated, no need to delete", configMap.Name)
		} else {
			c.ranktableStore.Delete(ranktableKey)
			klog.V(4).Infof("The ranktable of configMap %s has been successfully deleted", configMap.Name)
		}
	} else {
		klog.V(4).Infof("The ranktable of configMap %s has not been stored before, no need to delete", configMap.Name)
	}
}
