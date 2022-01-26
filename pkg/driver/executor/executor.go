// SPDX-FileCopyrightText: 2020 SAP SE or an SAP affiliate company and Gardener contributors
//
// SPDX-License-Identifier: Apache-2.0

package executor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gardener/machine-controller-manager-provider-openstack/pkg/apis/cloudprovider"
	api "github.com/gardener/machine-controller-manager-provider-openstack/pkg/apis/openstack"
	"github.com/gardener/machine-controller-manager-provider-openstack/pkg/client"

	"github.com/gophercloud/gophercloud/openstack/blockstorage/v2/volumes"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/extensions/bootfromvolume"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/extensions/keypairs"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/extensions/schedulerhints"
	"github.com/gophercloud/gophercloud/openstack/compute/v2/servers"
	"github.com/gophercloud/gophercloud/openstack/networking/v2/ports"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog"
	"k8s.io/utils/pointer"
)

// Executor concretely handles the execution of requests to the machine controller. Executor is responsible
// for communicating with OpenStack services and orchestrates the operations.
type Executor struct {
	Storage client.Storage
	Compute client.Compute
	Network client.Network
	Config  *api.MachineProviderConfig
}

// NewExecutor returns a new instance of Executor.
func NewExecutor(factory *client.Factory, config *api.MachineProviderConfig) (*Executor, error) {
	computeClient, err := factory.Compute(client.WithRegion(config.Spec.Region))
	if err != nil {
		klog.Errorf("failed to create compute client for executor: %v", err)
		return nil, err
	}
	networkClient, err := factory.Network(client.WithRegion(config.Spec.Region))
	if err != nil {
		klog.Errorf("failed to create network client for executor: %v", err)
		return nil, err
	}
	storageClient, err := factory.Storage(client.WithRegion(config.Spec.Region))
	if err != nil {
		klog.Errorf("failed to create network client for executor: %v", err)
		return nil, err
	}
	ex := &Executor{
		Storage: storageClient,
		Compute: computeClient,
		Network: networkClient,
		Config:  config,
	}
	return ex, nil
}

// CreateMachine creates a new OpenStack server instance and waits until it reports "ACTIVE".
// If there is an error during the build process, or if the building phase timeouts, it will delete any artifacts created.
func (ex *Executor) CreateMachine(ctx context.Context, machineName string, userData []byte) (string, error) {
	var (
		server *servers.Server
		err    error
	)

	deleteOnFail := func(err error) error {
		klog.Infof("attempting to delete server [Name=%q] after unsuccessful create operation with error: %v", machineName, err)
		if errIn := ex.DeleteMachine(ctx, machineName, ""); errIn != nil {
			return fmt.Errorf("error deleting server [Name=%q] after unsuccessful creation attempt: %v. Original error: %w", machineName, errIn, err)
		}
		if !isEmptyString(ex.Config.Spec.VolumeType) {
			var volume volumes.Volume
			var errChk, errDel error
			if volume, errChk = ex.checkBootVolume(machineName); errChk != nil && !client.IsNotFoundError(errChk) {
				return fmt.Errorf("error checking volume [ID=%q]: %v. Original error: %v", machineName, errChk, err)
			}

			volOpts := volumes.DeleteOpts{Cascade: true}
			if !client.IsNotFoundError(errChk) {
				errDel = ex.Storage.DeleteVolume(volume.ID, volOpts)
				if errDel != nil {
					return fmt.Errorf("error deleting volume [ID=%q]: %v. Original error: %v", machineName, errDel, err)
				}
			}
		}
		return err
	}

	server, err = ex.getMachineByName(ctx, machineName)
	if err == nil {
		klog.Infof("found existing server [Name=%q, ID=%q]", machineName, server.ID)
	} else if !errors.Is(err, ErrNotFound) {
		return "", err
	} else {
		// clean-up function when creation fails in an intermediate step
		serverNetworks, err := ex.resolveServerNetworks(ctx, machineName)
		if err != nil {
			return "", deleteOnFail(fmt.Errorf("failed to resolve server [Name=%q] networks: %w", machineName, err))
		}

		server, err = ex.deployServer(machineName, userData, serverNetworks)
		if err != nil {
			return "", deleteOnFail(fmt.Errorf("failed to deploy server [Name=%q]: %w", machineName, err))
		}
	}

	err = ex.waitForStatus(server.ID, []string{client.ServerStatusBuild}, []string{client.ServerStatusActive}, 600)
	if err != nil {
		return "", deleteOnFail(fmt.Errorf("error waiting for server [ID=%q] to reach target status: %w", server.ID, err))
	}

	if err := ex.patchServerPortsForPodNetwork(server.ID); err != nil {
		return "", deleteOnFail(fmt.Errorf("failed to patch server [ID=%q] ports: %s", server.ID, err))
	}

	return encodeProviderID(ex.Config.Spec.Region, server.ID), nil
}

func (ex *Executor) getSubnetIDs() []string {
	var subnetList []string

	subnetList = append(subnetList, ex.Config.Spec.SubnetIDs...)
	if ex.Config.Spec.SubnetID != nil {
		subnetList = append(subnetList, *ex.Config.Spec.SubnetID)
	}

	return sets.NewString(subnetList...).List()
}

// resolveServerNetworks resolves the network configuration for the server.
func (ex *Executor) resolveServerNetworks(ctx context.Context, machineName string) ([]servers.Network, error) {
	var (
		networkID      = ex.Config.Spec.NetworkID
		networks       = ex.Config.Spec.Networks
		subnetIDs      = ex.getSubnetIDs()
		serverNetworks = make([]servers.Network, 0)
	)

	klog.V(3).Infof("resolving network setup for machine [Name=%q]", machineName)
	// If SubnetID is specified in addition to NetworkID, we have to preallocate a Neutron Port to force the VMs to get IP from the subnet's range.
	if ex.isUserManagedNetwork() {
		// check if the subnets exists
		for _, subnetID := range subnetIDs {
			if _, err := ex.Network.GetSubnet(subnetID); err != nil {
				return nil, err
			}
		}

		klog.V(3).Infof("deploying machine [Name=%q] in subnet [ID=%q]", machineName, subnetIDs)
		portID, err := ex.getOrCreatePort(ctx, machineName)
		if err != nil {
			return nil, err
		}

		serverNetworks = append(serverNetworks, servers.Network{UUID: ex.Config.Spec.NetworkID, Port: portID})
		return serverNetworks, nil
	}

	if !isEmptyString(pointer.StringPtr(networkID)) {
		klog.V(3).Infof("deploying in network [ID=%q]", networkID)
		serverNetworks = append(serverNetworks, servers.Network{UUID: ex.Config.Spec.NetworkID})
		return serverNetworks, nil
	}

	for _, network := range networks {
		var (
			resolvedNetworkID string
			err               error
		)
		if isEmptyString(pointer.StringPtr(network.Id)) {
			resolvedNetworkID, err = ex.Network.NetworkIDFromName(network.Name)
			if err != nil {
				return nil, err
			}
		} else {
			resolvedNetworkID = network.Id
		}
		serverNetworks = append(serverNetworks, servers.Network{UUID: resolvedNetworkID})
	}
	return serverNetworks, nil
}

// waitForStatus blocks until the server with the specified ID reaches one of the target status.
// waitForStatus will fail if an error occurs, the operation it timeouts after the specified time, or the server status is not in the pending list.
func (ex *Executor) waitForStatus(serverID string, pending []string, target []string, secs int) error {
	return wait.Poll(5*time.Second, time.Duration(secs)*time.Second, func() (done bool, err error) {
		current, err := ex.Compute.GetServer(serverID)
		if err != nil {
			if client.IsNotFoundError(err) && strSliceContains(target, client.ServerStatusDeleted) {
				return true, nil
			}
			return false, err
		}

		klog.V(5).Infof("waiting for server [ID=%q] and current status %v, to reach status %v.", serverID, current.Status, target)
		if strSliceContains(target, current.Status) {
			return true, nil
		}

		// if there is no pending statuses defined or current status is in the pending list, then continue polling
		if len(pending) == 0 || strSliceContains(pending, current.Status) {
			return false, nil
		}

		retErr := fmt.Errorf("server [ID=%q] reached unexpected status %q", serverID, current.Status)
		if current.Status == client.ServerStatusError {
			retErr = fmt.Errorf("%s, fault: %+v", retErr, current.Fault)
		}

		return false, retErr
	})
}

// waitForVolumeStatus blocks until the server with the specified ID reaches one of the target status.
// waitForVolumeStatus will fail if an error occurs, the operation it timeouts after the specified time, or the volume status is not in the pending list.
func (ex *Executor) waitForVolumeStatus(volumeID string, pending []string, target []string, secs int) error {
	return wait.Poll(5*time.Second, time.Duration(secs)*time.Second, func() (done bool, err error) {
		current, err := ex.Storage.GetVolume(volumeID)
		if err != nil {
			if client.IsNotFoundError(err) && strSliceContains(target, client.VolumeStatusDeleting) {
				return true, nil
			}
			return false, err
		}

		klog.V(5).Infof("waiting for volume [ID=%q] and current status %v, to reach status %v.", volumeID, current.Status, target)
		if strSliceContains(target, current.Status) {
			return true, nil
		}

		// if there is no pending statuses defined or current status is in the pending list, then continue polling
		if len(pending) == 0 || strSliceContains(pending, current.Status) {
			return false, nil
		}

		retErr := fmt.Errorf("volume [ID=%q] reached unexpected status %q", volumeID, current.Status)
		if current.Status == client.VolumeStatusError {
			retErr = fmt.Errorf("%s", retErr)
		}

		return false, retErr
	})
}

// deployServer handles creating the server instance.
func (ex *Executor) deployServer(machineName string, userData []byte, nws []servers.Network) (*servers.Server, error) {
	keyName := ex.Config.Spec.KeyName
	imageName := ex.Config.Spec.ImageName
	imageID := ex.Config.Spec.ImageID
	securityGroups := ex.Config.Spec.SecurityGroups
	availabilityZone := ex.Config.Spec.AvailabilityZone
	metadata := ex.Config.Spec.Tags
	rootDiskSize := ex.Config.Spec.RootDiskSize
	useConfigDrive := ex.Config.Spec.UseConfigDrive
	flavorName := ex.Config.Spec.FlavorName
	volumeType := ex.Config.Spec.VolumeType

	var (
		imageRef   string
		createOpts servers.CreateOptsBuilder
		err        error
	)

	// use imageID if provided, otherwise try to resolve the imageName to an imageID
	if imageID != "" {
		imageRef = imageID
	} else {
		imageRef, err = ex.Compute.ImageIDFromName(imageName)
		if err != nil {
			return nil, fmt.Errorf("error resolving image ID from image name %q: %v", imageName, err)
		}
	}
	flavorRef, err := ex.Compute.FlavorIDFromName(flavorName)
	if err != nil {
		return nil, fmt.Errorf("error resolving flavor ID from flavor name %q: %v", imageName, err)
	}

	createOpts = &servers.CreateOpts{
		Name:             machineName,
		FlavorRef:        flavorRef,
		ImageRef:         imageRef,
		Networks:         nws,
		SecurityGroups:   securityGroups,
		Metadata:         metadata,
		UserData:         userData,
		AvailabilityZone: availabilityZone,
		ConfigDrive:      useConfigDrive,
	}

	createOpts = &keypairs.CreateOptsExt{
		CreateOptsBuilder: createOpts,
		KeyName:           keyName,
	}

	if ex.Config.Spec.ServerGroupID != nil {
		hints := schedulerhints.SchedulerHints{
			Group: *ex.Config.Spec.ServerGroupID,
		}
		createOpts = schedulerhints.CreateOptsExt{
			CreateOptsBuilder: createOpts,
			SchedulerHints:    hints,
		}
	}

	var volume volumes.Volume

	// If a custom block_device (root disk size is provided) we need to boot from volume
	if rootDiskSize > 0 {
		var blockDevices []bootfromvolume.BlockDevice

		if volumeType == nil {
			blockDevices, err = resourceInstanceBlockDevicesV2(rootDiskSize, imageRef, nil)
			if err != nil {
				return nil, err
			}
		} else {
			// volumeType is defined, so we have to create the volume beforehand and
			// add it to bootfromvolume.BlockDevice

			// check if volume already created
			volume, err = ex.checkBootVolume(machineName)
			if err != nil {
				return nil, err
			}

			// if not created before, create now
			if volume.ID == "" {
				klog.V(3).Infof("creating boot volume for %s", machineName)
				volume, err = ex.createBootVolume(rootDiskSize, volumeType, availabilityZone, imageRef, machineName)
				if err != nil {
					volerr := ex.Storage.DeleteVolume(volume.ID, volumes.DeleteOpts{Cascade: true})
					return &servers.Server{}, fmt.Errorf("error volume creation, %s and deletion %s", err, volerr)
				}
			}
			err = ex.waitForVolumeStatus(volume.ID, []string{client.VolumeStatusDownloading, client.VolumeStatusCreating}, []string{client.VolumeStatusAvailable}, 600)
			if err != nil {
				volerr := ex.Storage.DeleteVolume(volume.ID, volumes.DeleteOpts{Cascade: true})
				return &servers.Server{}, fmt.Errorf("error waiting for volume, %s and deletion %s", err, volerr)
			}

			blockDevices, err = resourceInstanceBlockDevicesV2(rootDiskSize, imageRef, &volume.ID)
			if err != nil {
				volerr := ex.Storage.DeleteVolume(volume.ID, volumes.DeleteOpts{Cascade: true})
				return &servers.Server{}, fmt.Errorf("error blockdevice creation, %s and deletion %s", err, volerr)
			}
		}

		createOpts = &bootfromvolume.CreateOptsExt{
			CreateOptsBuilder: createOpts,
			BlockDevice:       blockDevices,
		}
		return ex.Compute.BootFromVolume(createOpts)
	}

	return ex.Compute.CreateServer(createOpts)
}

func (ex *Executor) createBootVolume(size int, volumeType *string, zone string, imageRef string, name string) (volumes.Volume, error) {
	createOpts := volumes.CreateOpts{
		Size:             size,
		AvailabilityZone: zone,
		Name:             name,
		ImageID:          imageRef,
		VolumeType:       *volumeType,
	}

	volume, err := ex.Storage.CreateVolume(createOpts)
	if err != nil {
		return volumes.Volume{}, err
	}

	return *volume, nil
}

func (ex *Executor) checkBootVolume(name string) (res volumes.Volume, err error) {
	opts := volumes.ListOpts{
		Name: name,
	}
	volume, err := ex.Storage.ListVolumes(opts)
	for _, vol := range volume {
		if vol.Name == name {
			return vol, nil
		}
	}
	return volumes.Volume{}, nil
}

func resourceInstanceBlockDevicesV2(rootDiskSize int, imageID string, volumeID *string) ([]bootfromvolume.BlockDevice, error) {
	blockDeviceOpts := make([]bootfromvolume.BlockDevice, 1)
	if volumeID != nil {
		blockDeviceOpts[0] = bootfromvolume.BlockDevice{
			DeleteOnTermination: true,
			DestinationType:     bootfromvolume.DestinationVolume,
			SourceType:          bootfromvolume.SourceVolume,
			UUID:                *volumeID,
			BootIndex:           0,
		}

	} else {
		blockDeviceOpts[0] = bootfromvolume.BlockDevice{
			UUID:                imageID,
			VolumeSize:          rootDiskSize,
			BootIndex:           0,
			DeleteOnTermination: true,
			SourceType:          "image",
			DestinationType:     "volume",
		}
	}
	klog.V(3).Infof("[DEBUG] Block Device Options: %+v", blockDeviceOpts)
	return blockDeviceOpts, nil
}

// patchServerPortsForPodNetwork updates a server's ports with rules for whitelisting the pod network CIDR.
func (ex *Executor) patchServerPortsForPodNetwork(serverID string) error {
	allPorts, err := ex.Network.ListPorts(&ports.ListOpts{
		DeviceID: serverID,
	})
	if err != nil {
		return fmt.Errorf("failed to get ports: %v", err)
	}

	if len(allPorts) == 0 {
		return fmt.Errorf("got an empty port list for server %q", serverID)
	}

	podNetworkIDs, err := ex.resolveNetworkIDsForPodNetwork()
	if err != nil {
		return fmt.Errorf("failed to resolve network IDs for the pod network %v", err)
	}

	for _, port := range allPorts {
		if podNetworkIDs.Has(port.NetworkID) {
			addressPairFound := false

			for _, pair := range port.AllowedAddressPairs {
				for _, podCidr := range strings.Split(ex.Config.Spec.PodNetworkCidr, ",") {
					if pair.IPAddress == podCidr {
						klog.V(3).Infof("port [ID=%q] already allows pod network CIDR range. Skipping update...", port.ID)
						addressPairFound = true
						// break inner loop if target found
						break
					}
				}
			}
			// continue outer loop if target found
			if addressPairFound {
				continue
			}

			var allowedAddressPairs []ports.AddressPair
			for _, podCidr := range strings.Split(ex.Config.Spec.PodNetworkCidr, ",") {
				allowedAddressPairs = append(allowedAddressPairs, ports.AddressPair{IPAddress: podCidr})
			}
			if err := ex.Network.UpdatePort(port.ID, ports.UpdateOpts{
				AllowedAddressPairs: &allowedAddressPairs,
			}); err != nil {
				return fmt.Errorf("failed to update allowed address pair for port [ID=%q]: %v", port.ID, err)

			}
		}
	}
	return nil
}

// resolveNetworkIDsForPodNetwork resolves the networks that accept traffic from the pod CIDR range.
func (ex *Executor) resolveNetworkIDsForPodNetwork() (sets.String, error) {
	var (
		networkID     = ex.Config.Spec.NetworkID
		networks      = ex.Config.Spec.Networks
		podNetworkIDs = sets.NewString()
	)

	if !isEmptyString(pointer.StringPtr(networkID)) {
		podNetworkIDs.Insert(networkID)
		return podNetworkIDs, nil
	}

	for _, network := range networks {
		var (
			resolvedNetworkID string
			err               error
		)
		if isEmptyString(pointer.StringPtr(network.Id)) {
			resolvedNetworkID, err = ex.Network.NetworkIDFromName(network.Name)
			if err != nil {
				return nil, err
			}
		} else {
			resolvedNetworkID = network.Id
		}
		if network.PodNetwork {
			podNetworkIDs.Insert(resolvedNetworkID)
		}
	}
	return podNetworkIDs, nil
}

// DeleteMachine deletes a server based on the supplied machineName. If a providerID is supplied it is used instead of the
// machineName to locate the server.
func (ex *Executor) DeleteMachine(ctx context.Context, machineName, providerID string) error {
	var (
		server *servers.Server
		err    error
	)

	if !isEmptyString(pointer.StringPtr(providerID)) {
		serverID := decodeProviderID(providerID)
		server, err = ex.getMachineByID(ctx, serverID)
	} else {
		server, err = ex.getMachineByName(ctx, machineName)
	}

	if err == nil {
		klog.V(1).Infof("deleting server [Name=%s, ID=%s]", server.Name, server.ID)
		if err := ex.Compute.DeleteServer(server.ID); err != nil {
			return err
		}

		if err = ex.waitForStatus(server.ID, nil, []string{client.ServerStatusDeleted}, 300); err != nil {
			return fmt.Errorf("error while waiting for server [ID=%q] to be deleted: %v", server.ID, err)
		}
	} else if !errors.Is(err, ErrNotFound) {
		return err
	}
	if !isEmptyString(ex.Config.Spec.VolumeType) {
		var volume volumes.Volume
		if volume, err = ex.checkBootVolume(machineName); err != nil && !client.IsNotFoundError(err) {
			return fmt.Errorf("error checking volume [ID=%q]: %v", machineName, err)
		}

		volOpts := volumes.DeleteOpts{Cascade: true}
		if client.IsNotFoundError(err) {
			err := ex.Storage.DeleteVolume(volume.ID, volOpts)
			if err != nil {
				return fmt.Errorf("error deleting volume [ID=%q]: %v", machineName, err)
			}
		}
	}
	if ex.isUserManagedNetwork() {
		return ex.deletePort(ctx, machineName)
	}

	return nil
}

func (ex *Executor) getOrCreatePort(_ context.Context, machineName string) (string, error) {
	var (
		err              error
		securityGroupIDs []string
	)

	portID, err := ex.Network.PortIDFromName(machineName)
	if err == nil {
		klog.V(2).Infof("found port [Name=%q, ID=%q]... skipping creation", machineName, portID)
		return portID, nil
	}

	if !client.IsNotFoundError(err) {
		klog.V(5).Infof("error fetching port [Name=%q]: %s", machineName, err)
		return "", fmt.Errorf("error fetching port [Name=%q]: %s", machineName, err)
	}

	klog.V(5).Infof("port [Name=%q] does not exist", machineName)
	klog.V(3).Infof("creating port [Name=%q]... ", machineName)

	for _, securityGroup := range ex.Config.Spec.SecurityGroups {
		securityGroupID, err := ex.Network.GroupIDFromName(securityGroup)
		if err != nil {
			return "", err
		}
		securityGroupIDs = append(securityGroupIDs, securityGroupID)
	}

	var allowedAddressPairs []ports.AddressPair
	for _, podCidr := range strings.Split(ex.Config.Spec.PodNetworkCidr, ",") {
		allowedAddressPairs = append(allowedAddressPairs, ports.AddressPair{IPAddress: podCidr})
	}

	portIPs := make([]ports.IP, 0)
	for _, subnetID := range ex.getSubnetIDs() {
		portIPs = append(portIPs, ports.IP{
			SubnetID: subnetID,
		})
	}

	port, err := ex.Network.CreatePort(&ports.CreateOpts{
		Name:                machineName,
		NetworkID:           ex.Config.Spec.NetworkID,
		FixedIPs:            portIPs,
		AllowedAddressPairs: allowedAddressPairs,
		SecurityGroups:      &securityGroupIDs,
	})

	if err != nil {
		return "", err
	}

	klog.V(3).Infof("port [Name=%q] successfully created", port.Name)
	return port.ID, nil
}
func (ex *Executor) deletePort(_ context.Context, machineName string) error {
	portID, err := ex.Network.PortIDFromName(machineName)
	if err != nil {
		if client.IsNotFoundError(err) {
			klog.V(3).Infof("port [Name=%q] was not found", machineName)
			return nil
		}
		return fmt.Errorf("error deleting port [Name=%q]: %s", machineName, err)
	}

	klog.V(2).Infof("deleting port [Name=%q]", machineName)
	err = ex.Network.DeletePort(portID)
	if err != nil {
		klog.Errorf("failed to delete port [Name=%q]", machineName)
		return err
	}
	klog.V(3).Infof("deleted port [Name=%q]", machineName)

	return nil
}

// getMachineByProviderID fetches the data for a server based on a provider-encoded ID.
func (ex *Executor) getMachineByID(_ context.Context, serverID string) (*servers.Server, error) {
	klog.V(2).Infof("finding server with [ID=%q]", serverID)
	server, err := ex.Compute.GetServer(serverID)
	if err != nil {
		klog.V(2).Infof("error finding server [ID=%q]: %v", serverID, err)
		if client.IsNotFoundError(err) {
			// normalize errors by wrapping not found error
			return nil, fmt.Errorf("could not find server [ID=%q]: %w", serverID, ErrNotFound)
		}
		return nil, err
	}

	var (
		searchClusterName string
		searchNodeRole    string
	)
	for key := range ex.Config.Spec.Tags {
		if strings.Contains(key, cloudprovider.ServerTagClusterPrefix) {
			searchClusterName = key
		} else if strings.Contains(key, cloudprovider.ServerTagRolePrefix) {
			searchNodeRole = key
		}
	}

	if _, nameOk := server.Metadata[searchClusterName]; nameOk {
		if _, roleOk := server.Metadata[searchNodeRole]; roleOk {
			return server, nil
		}
	}

	klog.Warningf("server [ID=%q] found, but cluster/role tags are missing/not matching", serverID)
	return nil, fmt.Errorf("could not find server [ID=%q]: %w", serverID, ErrNotFound)
}

// getMachineByName returns a server that matches the following criteria:
// a) has the same name as machineName
// b) has the cluster and role tags as set in the machineClass
// The current approach is weak because the tags are currently stored as server metadata. Later Nova versions allow
// to store tags in a respective field and do a server-side filtering. To avoid incompatibility with older versions
// we will continue making the filtering clientside.
func (ex *Executor) getMachineByName(_ context.Context, machineName string) (*servers.Server, error) {
	var (
		searchClusterName string
		searchNodeRole    string
	)

	for key := range ex.Config.Spec.Tags {
		if strings.Contains(key, cloudprovider.ServerTagClusterPrefix) {
			searchClusterName = key
		} else if strings.Contains(key, cloudprovider.ServerTagRolePrefix) {
			searchNodeRole = key
		}
	}

	if searchClusterName == "" || searchNodeRole == "" {
		klog.Warningf("getMachineByName operation can not proceed: cluster/role tags are missing for machine [Name=%q]", machineName)
		return nil, fmt.Errorf("getMachineByName operation can not proceed: cluster/role tags are missing for machine [Name=%q]", machineName)
	}

	listedServers, err := ex.Compute.ListServers(&servers.ListOpts{
		Name: machineName,
	})
	if err != nil {
		return nil, err
	}

	var matchingServers []servers.Server
	for _, server := range listedServers {
		if server.Name == machineName {
			if _, nameOk := server.Metadata[searchClusterName]; nameOk {
				if _, roleOk := server.Metadata[searchNodeRole]; roleOk {
					matchingServers = append(matchingServers, server)
				}
			}
		}
	}

	if len(matchingServers) > 1 {
		return nil, fmt.Errorf("failed to find server [Name=%q]: %w", machineName, ErrMultipleFound)
	} else if len(matchingServers) == 0 {
		return nil, fmt.Errorf("failed to find server [Name=%q]: %w", machineName, ErrNotFound)
	}

	return &matchingServers[0], nil
}

// ListMachines lists returns a map from the server's encoded provider ID to the server name.
func (ex *Executor) ListMachines(ctx context.Context) (map[string]string, error) {
	allServers, err := ex.listServers(ctx)
	if err != nil {
		return nil, err
	}

	result := map[string]string{}
	for _, server := range allServers {
		providerID := encodeProviderID(ex.Config.Spec.Region, server.ID)
		result[providerID] = server.Name
	}

	return result, nil
}

// ListServers lists all servers with the appropriate tags.
func (ex *Executor) listServers(_ context.Context) ([]servers.Server, error) {
	searchClusterName := ""
	searchNodeRole := ""

	for key := range ex.Config.Spec.Tags {
		if strings.Contains(key, cloudprovider.ServerTagClusterPrefix) {
			searchClusterName = key
		} else if strings.Contains(key, cloudprovider.ServerTagRolePrefix) {
			searchNodeRole = key
		}
	}

	//
	if searchClusterName == "" || searchNodeRole == "" {
		klog.Warningf("operation can not proceed: cluster/role tags are missing")
		return nil, fmt.Errorf("operation can not proceed: cluster/role tags are missing")
	}

	allServers, err := ex.Compute.ListServers(&servers.ListOpts{})
	if err != nil {
		return nil, err
	}

	var result []servers.Server
	for _, server := range allServers {
		if _, nameOk := server.Metadata[searchClusterName]; nameOk {
			if _, roleOk := server.Metadata[searchNodeRole]; roleOk {
				result = append(result, server)
			}
		}
	}

	return result, nil
}

// isUserManagedNetwork returns true if the port used by the machine will be created and managed by MCM.
func (ex *Executor) isUserManagedNetwork() bool {
	return !isEmptyString(pointer.StringPtr(ex.Config.Spec.NetworkID)) && len(ex.getSubnetIDs()) != 0
}
