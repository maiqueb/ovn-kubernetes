package persistentips

import (
	"fmt"
	"net"
	"strings"

	"k8s.io/klog/v2"

	ipamclaimsapi "github.com/maiqueb/persistentips/pkg/crd/persistentip/v1alpha1"
	ipam "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/allocator/ip"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/allocator/ip/subnet"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

// Allocator acts on IPAMClaim events handed off by the cluster network
// controller and allocates or releases IPs for IPAMClaims.
type Allocator struct {
	kube kube.InterfaceOVN

	// ipAllocator of IPs within subnets
	ipAllocator subnet.NamedAllocator
}

// NewPersistentIPsAllocator builds a new PersistentIPsAllocator
func NewPersistentIPsAllocator(kube kube.InterfaceOVN, ipAllocator subnet.NamedAllocator) *Allocator {
	pipsAllocator := &Allocator{
		kube:        kube,
		ipAllocator: ipAllocator,
	}

	return pipsAllocator
}

// Delete releases persistent IPs previously allocated
func (a *Allocator) Delete(pips *ipamclaimsapi.IPAMClaim) error {
	ips, err := util.ParseIPNets(pips.Status.IPs)
	if err != nil {
		return fmt.Errorf("failed parsing ipnets releasing persistent IPs: %v", err)
	}
	if err := a.ipAllocator.ReleaseIPs(ips); err != nil {
		return fmt.Errorf("failed releasing persistent IPs: %v", err)
	}
	klog.V(5).Infof("Released IPs: %+v", ips)
	return nil
}

// Reconcile allocates or releases IPs for IPAMClaims updating its status
// with the IP addresses
func (a *Allocator) Reconcile(ipamClaim *ipamclaimsapi.IPAMClaim, ips []string) error {
	klog.V(5).Infof("Reconciling IPAMLease %q", ipamClaim.Name)
	if len(ipamClaim.Status.IPs) > 0 {
		klog.V(5).Infof("Already have neat lookin' IPs for: %q. Bail out !", ipamClaim.Name)
		return nil
	}

	if err := a.kube.UpdateIPAMLeaseIPs(ipamClaim, ips); err != nil {
		return fmt.Errorf(
			"failed to update the allocation %q with allocations %q: %v",
			ipamClaim.Name,
			strings.Join(ips, ","),
			err,
		)
	}

	return nil
}

// Sync initializes the allocator with persistentips that already exist on the cluster
func (a *Allocator) Sync(objs []interface{}) error {
	ips := []*net.IPNet{}
	for _, obj := range objs {
		pips, ok := obj.(*ipamclaimsapi.IPAMClaim)
		if !ok {
			klog.Errorf("Could not cast %T object to *ipamclaimsapi.IPAMLease", obj)
			continue
		}
		ipnets, err := util.ParseIPNets(pips.Status.IPs)
		if err != nil {
			return fmt.Errorf("failed at parsing IP when allocating persistent IPs: %v", err)
		}
		ips = append(ips, ipnets...)
	}
	if len(ips) > 0 {
		if err := a.ipAllocator.AllocateIPs(ips); err != nil && !ipam.IsErrAllocated(err) {
			return fmt.Errorf("failed allocating persistent ips: %v", err)
		}
	}
	return nil
}
