package ranktable

import (
	"encoding/json"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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

type EventKeyInfo struct {
	Namespace string
	Name      string
}

func convertObjToConfigMap(obj interface{}) *corev1.ConfigMap {
	unstructuredObj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil
	}
	configMap := &corev1.ConfigMap{}
	if err = runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj, configMap); err != nil {
		return nil
	}
	return configMap
}

func convertObjToPod(object runtime.Object) *corev1.Pod {
	unstructuredObj, ok := object.(*unstructured.Unstructured)
	if !ok {
		return nil
	}
	pod := &corev1.Pod{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(unstructuredObj.Object, pod); err != nil {
		return nil
	}
	return pod
}

func getSingleRanktableFromConfigmap(configMap *corev1.ConfigMap) *SingleRanktable {
	if configMap == nil {
		return nil
	}
	if len(configMap.Data) > 0 {
		if ranktableStr, exits := configMap.Data[MountJobStartHcclName]; exits {
			ranktable := &SingleRanktable{}
			if err := json.Unmarshal([]byte(ranktableStr), ranktable); err == nil {
				return ranktable
			}
		}
	}
	return nil
}

func getGlobalRanktableFromConfigMap(configMap *corev1.ConfigMap) *GlobalRanktable {
	return getSingleRanktableFromConfigmap(configMap)
}

func getSingleRanktableKeyByNamespaceAndName(namespace, name string) string {
	return fmt.Sprintf("%s-%s-ranktable", namespace, name)
}

func getGlobalNetworkLinksFromConfigMap(configMap *corev1.ConfigMap) *GlobalNetworkLinks {
	if configMap == nil {
		return nil
	}
	if len(configMap.Data) > 0 {
		if networkLinksStr, exits := configMap.Data[MountNetworkLinksName]; exits {
			globalNetworkLinks := &GlobalNetworkLinks{}
			if err := json.Unmarshal([]byte(networkLinksStr), globalNetworkLinks); err == nil {
				return globalNetworkLinks
			}
		}
	}
	return nil
}
