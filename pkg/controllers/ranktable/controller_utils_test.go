package ranktable

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalJson(t *testing.T) {
	tests := []struct {
		name               string
		globalNetworkLinks GlobalNetworkLinks
		expectedData       []byte
		expectedError      error
	}{
		{
			name: "Global Network Links with No Pod IP Maps",
			globalNetworkLinks: GlobalNetworkLinks{
				Status:      "initializing",
				DataVersion: 1,
				PodCount:    "0",
			},
		}, {
			name: "Global Network Links with Empty Pod IP Maps",
			globalNetworkLinks: GlobalNetworkLinks{
				Status:       "initializing",
				DataVersion:  2,
				PodCount:     "0",
				NetworkLinks: map[string]string{},
			},
		}, {
			name: "Global Network Links with Normal Pod IP Maps",
			globalNetworkLinks: GlobalNetworkLinks{
				Status:      "completed",
				DataVersion: 2,
				PodCount:    "4",
				NetworkLinks: map[string]string{
					"hyperjob-example-taskb-0-worker-0.zsbtest": "192.168.0.51",
					"hyperjob-example-taskb-1-worker-0.zsbtest": "192.168.0.52",
					"hyperjob-example-taska-0-worker-0.zsbtest": "192.168.0.41",
					"hyperjob-example-taska-1-worker-0.zsbtest": "192.168.0.42",
				},
			},
		},
	}
	for _, tt := range tests {
		tt.expectedData, tt.expectedError = json.Marshal(tt.globalNetworkLinks)
		t.Run(tt.name, func(t *testing.T) {
			data, err := tt.globalNetworkLinks.marshalJson()
			assert.Equal(t, tt.expectedData, data)
			assert.Equal(t, tt.expectedError, err)
		})
	}
}
