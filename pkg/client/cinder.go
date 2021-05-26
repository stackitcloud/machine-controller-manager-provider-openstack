package client

import (
	"fmt"
	"github.com/gardener/machine-controller-manager/pkg/util/provider/metrics"
	"github.com/gophercloud/gophercloud"
	"github.com/gophercloud/gophercloud/openstack"
	"github.com/gophercloud/gophercloud/openstack/blockstorage/v2/volumes"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	_ Storage = &cinderV2{}
)

func newCinderV2(providerClient *gophercloud.ProviderClient, eo gophercloud.EndpointOpts) (*cinderV2, error) {
	storage, err := openstack.NewBlockStorageV2(providerClient, eo)
	if err != nil {
		return nil, fmt.Errorf("could not initialize storage client: %v", err)
	}
	return &cinderV2{
		serviceClient: storage,
	}, nil
}

type cinderV2 struct {
	serviceClient *gophercloud.ServiceClient
}

func (c cinderV2) GetVolume(id string) (*volumes.Volume, error) {
	volume, err := volumes.Get(c.serviceClient, id).Extract()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil {
		if !IsNotFoundError(err) {
			metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		}
		return nil, err
	}
	return volume, nil
}

func (c cinderV2) CreateVolume(opts volumes.CreateOptsBuilder) (*volumes.Volume, error) {
	volume, err := volumes.Create(c.serviceClient, opts).Extract()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil {
		metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		return nil, err
	}

	return volume, nil
}

func (c cinderV2) ListVolumes(opts volumes.ListOptsBuilder) ([]volumes.Volume, error) {
	pages, err := volumes.List(c.serviceClient, opts).AllPages()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil {
		metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		return nil, err
	}
	return volumes.ExtractVolumes(pages)
}

func (c cinderV2) UpdateVolume(id string, opts volumes.UpdateOptsBuilder) (*volumes.Volume, error) {
	volume, err := volumes.Update(c.serviceClient, id, opts).Extract()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil && !IsNotFoundError(err) {
		metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		return nil, err
	}
	return volume, nil
}

func (c cinderV2) DeleteVolume(id string, opts volumes.DeleteOptsBuilder) error {
	err := volumes.Delete(c.serviceClient, id, opts).ExtractErr()

	metrics.APIRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
	if err != nil && !IsNotFoundError(err) {
		metrics.APIFailedRequestCount.With(prometheus.Labels{"provider": "openstack", "service": "cinder"}).Inc()
		return err
	}
	return nil
}

func (c cinderV2) VolumeIDFromName(name string) (string, error) {
	return volumes.IDFromName(c.serviceClient, name)
}
