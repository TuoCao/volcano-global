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

type GlobalNetworkLinks struct {
	Status       string            `json:"status"`
	DataVersion  int               `json:"data_version"`
	PodCount     string            `json:"pod_count"`
	NetworkLinks map[string]string `json:"ips,omitempty"`
}
