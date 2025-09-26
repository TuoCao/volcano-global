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
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
	trainingv1alpha1 "volcano.sh/apis/pkg/apis/training/v1alpha1"
)

var (
	JobGroupVersionKind = schema.GroupVersionKind{
		Group:   batchv1alpha1.SchemeGroupVersion.Group,
		Version: batchv1alpha1.SchemeGroupVersion.Version,
		Kind:    "Job",
	}
	HyperJobGroupVersionKind = schema.GroupVersionKind{
		Group:   trainingv1alpha1.SchemeGroupVersion.Group,
		Version: trainingv1alpha1.SchemeGroupVersion.Version,
		Kind:    "HyperJob",
	}
	ConfigMapGroupVersionResource = corev1.SchemeGroupVersion.WithResource("configmaps")
	PodGroupVersionResource       = corev1.SchemeGroupVersion.WithResource("pods")
)

func NewGlobalRanktable() *GlobalRanktable {
	return &GlobalRanktable{
		Status:       "",
		Version:      RanktableVersion,
		DataVersion:  0,
		ServerCount:  "",
		ServerList:   []ServerBase{},
		SuperPodList: []SuperPodBase{},
		ClusterList:  []ClusterBase{},
	}
}

func NewGlobalNetworkLinks() *GlobalNetworkLinks {
	return &GlobalNetworkLinks{
		Status:       "",
		DataVersion:  0,
		PodCount:     "0",
		NetworkLinks: make(map[string]string),
	}
}

type SyncEvent struct {
	Namespace string
	Name      string
}

func (s *SyncEvent) getKey() string {
	return fmt.Sprintf("%s/%s", s.Namespace, s.Name)
}

func convertObjToConfigMap(obj interface{}) (*corev1.ConfigMap, error) {
	unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, err
	}
	configMap := &corev1.ConfigMap{}
	if err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj, configMap); err != nil {
		return nil, err
	}
	return configMap, nil
}

func getSingleRanktableFromConfigMap(configMap *corev1.ConfigMap) (*SingleRanktable, error) {
	if configMap == nil {
		return nil, fmt.Errorf("configMap is nil")
	}
	if len(configMap.Data) == 0 {
		return nil, fmt.Errorf("configMap %s has no data", configMap.Name)
	}
	if ranktableStr, exists := configMap.Data[MountJobStartHcclName]; !exists {
		return nil, fmt.Errorf("configMap %s has no ranktable", configMap.Name)
	} else {
		ranktable := &SingleRanktable{}
		if err := json.Unmarshal([]byte(ranktableStr), ranktable); err != nil {
			return nil, err
		} else {
			return ranktable, nil
		}
	}
}

func getGlobalRanktableFromConfigMap(configMap *corev1.ConfigMap) (*GlobalRanktable, error) {
	return getSingleRanktableFromConfigMap(configMap)
}

func getSingleRanktableKeyByJobNamespaceAndName(namespace, name string) string {
	return fmt.Sprintf("%s-%s-ranktable", namespace, name)
}

func getGlobalNetworkLinksFromConfigMap(configMap *corev1.ConfigMap) (*GlobalNetworkLinks, error) {
	if configMap == nil {
		return nil, fmt.Errorf("configMap is nil")
	}
	if len(configMap.Data) == 0 {
		return nil, fmt.Errorf("configMap %s has no data", configMap.Name)
	}
	if networkLinksStr, exists := configMap.Data[MountNetworkLinksName]; !exists {
		return nil, fmt.Errorf("configMap %s has no network links", configMap.Name)
	} else {
		globalNetworkLinks := &GlobalNetworkLinks{}
		if err := json.Unmarshal([]byte(networkLinksStr), globalNetworkLinks); err != nil {
			return nil, err
		} else {
			return globalNetworkLinks, nil
		}
	}
}

func getJobOwner(job *batchv1alpha1.Job) (*metav1.OwnerReference, error) {
	if job == nil {
		return nil, fmt.Errorf("job is nil")
	}
	for _, owner := range job.OwnerReferences {
		ownerGV, err := schema.ParseGroupVersion(owner.APIVersion)
		if err != nil {
			klog.V(4).Infof("Failed to parse APIVersion %s for job %s, err: %v", owner.APIVersion, job.Name, err)
			continue
		}
		ownerGVK := schema.GroupVersionKind{
			Group:   ownerGV.Group,
			Version: ownerGV.Version,
			Kind:    owner.Kind,
		}
		if ownerGVK == HyperJobGroupVersionKind {
			return &owner, nil
		}
	}
	return nil, fmt.Errorf("job %s does not have a wanted owner reference", job.Name)
}

func getConfigMapOwner(configMap *corev1.ConfigMap) (*metav1.OwnerReference, error) {
	if configMap == nil {
		return nil, fmt.Errorf("configMap is nil")
	}
	for _, owner := range configMap.OwnerReferences {
		ownerGV, err := schema.ParseGroupVersion(owner.APIVersion)
		if err != nil {
			klog.V(4).Infof("Failed to parse APIVersion %s for configmap %s, err: %v", owner.APIVersion, configMap.Name, err)
			continue
		}
		ownerGVK := schema.GroupVersionKind{
			Group:   ownerGV.Group,
			Version: ownerGV.Version,
			Kind:    owner.Kind,
		}
		if ownerGVK == JobGroupVersionKind {
			return &owner, nil
		}
	}
	return nil, fmt.Errorf("configMap %s does not have a wanted owner reference", configMap.Name)
}
