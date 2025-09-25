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
	"k8s.io/klog/v2"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"

	batchv1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

// SingleRanktable stores a specific ranktable for one vcjob in one cluster
type SingleRanktable struct {
	Status       string         `json:"status"`
	Version      string         `json:"version"`
	DataVersion  int            `json:"data_version,omitempty"`
	ServerCount  string         `json:"server_count"`
	ServerList   []ServerBase   `json:"server_list"`
	SuperPodList []SuperPodBase `json:"super_pod_list,omitempty"`
	ClusterList  []ClusterBase  `json:"cluster_list,omitempty"`
}

type ClusterBase struct {
	ClusterId    string         `json:"cluster_id"`
	AZId         string         `json:"az_id"`
	RegionId     string         `json:"region_id"`
	SuperPodList []SuperPodBase `json:"super_pod_list"`
}

type ServerBase struct {
	ServerId string       `json:"server_id"`
	Device   []DeviceBase `json:"device"`
}

type DeviceBase struct {
	DeviceId      string `json:"device_id"`
	SuperDeviceId string `json:"super_device_id,omitempty"`
	DeviceIp      string `json:"device_ip"`
	RankId        string `json:"rank_id"`
	TorIp         string `json:"tor_ip,omitempty"`
	TorPort       string `json:"tor_port,omitempty"`
	DpuIp         string `json:"dpu_ip,omitempty"`
	NumaId        string `json:"numa_id,omitempty"`
}

type SuperPodBase struct {
	SuperPodId string           `json:"super_pod_id"`
	ServerList []SuperPodServer `json:"server_list,omitempty"`
}

type SuperPodServer struct {
	ServerId string `json:"server_id"`
}

// TorList stores a specific tor list for one vc job in one cluster
type TorList struct {
	Status      string      `json:"status"`
	Version     string      `json:"version"`
	DataVersion int         `json:"data_version,omitempty"`
	ServerCount string      `json:"server_count"`
	ServerList  []TorServer `json:"server_list"`
}

type TorServer struct {
	PodName  string      `json:"pod_name"`
	ServerId string      `json:"server_id"`
	Device   []TorDevice `json:"device"`
}

type TorDevice struct {
	DeviceId string `json:"device_id"`
	DeviceIp string `json:"device_ip"`
	TorIp    string `json:"tor_ip"`
	TorPort  string `json:"tor_port"`
}

type SingleRanktableInfo struct {
	clusterId string
	jobName   string
	ranktable *SingleRanktable
}

type GlobalRanktable = SingleRanktable

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

type GlobalNetworkLinks struct {
	Status       string            `json:"status"`
	DataVersion  int               `json:"data_version"`
	PodCount     string            `json:"pod_count"`
	NetworkLinks map[string]string `json:"ips,omitempty"`
}

func NewGlobalNetworkLinks() *GlobalNetworkLinks {
	return &GlobalNetworkLinks{
		Status:       "",
		DataVersion:  0,
		PodCount:     "0",
		NetworkLinks: make(map[string]string),
	}
}

var (
	JobGroupVersionKind = schema.GroupVersionKind{
		Group:   "batch.volcano.sh",
		Version: "v1alpha1",
		Kind:    "Job",
	}
	HyperJobGroupVersionKind = schema.GroupVersionKind{
		Group:   "training.volcano.sh",
		Version: "v1alpha1",
		Kind:    "HyperJob",
	}
	ConfigMapGroupVersionResource = corev1.SchemeGroupVersion.WithResource("configmaps")
	PodGroupVersionResource       = corev1.SchemeGroupVersion.WithResource("pods")
)

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
