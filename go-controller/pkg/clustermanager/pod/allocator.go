package pod

import (
	"fmt"
	"net"
	"sync"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/klog/v2"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	persistentipsapi "github.com/maiqueb/persistentips/pkg/crd/persistentip/v1alpha1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/allocator/id"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/allocator/ip/subnet"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/allocator/pod"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/clustermanager/persistentips"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

// PodAllocator acts on pods events handed off by the cluster network controller
// and allocates or releases resources (IPs and tunnel IDs at the time of this
// writing) to pods on behalf of cluster manager.
type PodAllocator struct {
	netInfo util.NetInfo

	kube         kube.InterfaceOVN
	watchFactory *factory.WatchFactory

	// ipAllocator of IPs within subnets
	ipAllocator subnet.Allocator

	// idAllocator of IDs within the network
	idAllocator id.Allocator

	// An utility to allocate the PodAnnotation to pods
	podAnnotationAllocator *pod.PodAnnotationAllocator

	// track pods that have been released but not deleted yet so that we don't
	// release more than once
	releasedPods      map[string]sets.Set[string]
	releasedPodsMutex sync.Mutex
}

// NewPodAllocator builds a new PodAllocator
func NewPodAllocator(netInfo util.NetInfo, podLister listers.PodLister, kube kube.InterfaceOVN, watchFactory *factory.WatchFactory) *PodAllocator {
	podAnnotationAllocator := pod.NewPodAnnotationAllocator(
		netInfo,
		podLister,
		kube,
	)

	podAllocator := &PodAllocator{
		netInfo:                netInfo,
		kube:                   kube,
		watchFactory:           watchFactory,
		releasedPods:           map[string]sets.Set[string]{},
		releasedPodsMutex:      sync.Mutex{},
		podAnnotationAllocator: podAnnotationAllocator,
	}

	// this network might not have IPAM, we will just allocate MAC addresses
	if util.DoesNetworkRequireIPAM(netInfo) {
		podAllocator.ipAllocator = subnet.NewAllocator()
	}

	return podAllocator
}

func (a *PodAllocator) IPAllocator() subnet.NamedAllocator {
	return a.ipAllocator.ForSubnet(a.netInfo.GetNetworkName())
}

// Init initializes the allocator with as configured for the network
func (a *PodAllocator) Init() error {
	var err error
	if util.DoesNetworkRequireTunnelIDs(a.netInfo) {
		a.idAllocator, err = id.NewIDAllocator(a.netInfo.GetNetworkName(), types.MaxLogicalPortTunnelKey)
		if err != nil {
			return err
		}
		// Reserve the id 0. We don't want to assign this id to any of the pods.
		err = a.idAllocator.ReserveID("zero", 0)
		if err != nil {
			return err
		}
	}

	if util.DoesNetworkRequireIPAM(a.netInfo) {
		subnets := a.netInfo.Subnets()
		ipNets := make([]*net.IPNet, 0, len(subnets))
		for _, subnet := range subnets {
			ipNets = append(ipNets, subnet.CIDR)
		}

		return a.ipAllocator.AddOrUpdateSubnet(a.netInfo.GetNetworkName(), ipNets, a.netInfo.ExcludeSubnets()...)
	}

	return nil
}

// Reconcile allocates or releases IPs for pods updating the pod annotation
// as necessary with all the additional information derived from those IPs
func (a *PodAllocator) Reconcile(old, new *corev1.Pod) error {
	releaseFromAllocator := true
	return a.reconcile(old, new, releaseFromAllocator)
}

// Sync initializes the allocator with pods that already exist on the cluster
func (a *PodAllocator) Sync(objs []interface{}) error {
	// on sync, we don't release IPs from the allocator, we are just trying to
	// allocate annotated IPs; specifically we don't want to release IPs of
	// completed pods that might be being used by other pods
	releaseFromAllocator := false

	for _, obj := range objs {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			klog.Errorf("Could not cast %T object to *corev1.Pod", obj)
			continue
		}
		err := a.reconcile(nil, pod, releaseFromAllocator)
		if err != nil {
			klog.Errorf("Failed to sync pod %s/%s: %v", pod.Namespace, pod.Name, err)
		}
	}

	return nil
}

func (a *PodAllocator) reconcile(old, new *corev1.Pod, releaseFromAllocator bool) error {
	var pod *corev1.Pod
	if old != nil {
		pod = old
	}
	if new != nil {
		pod = new
	}

	podScheduled := util.PodScheduled(pod)
	podWantsHostNetwork := util.PodWantsHostNetwork(pod)

	// nothing to do for a unscheduled or host network pods
	if !podScheduled || podWantsHostNetwork {
		return nil
	}

	onNetwork, networkMap, err := util.GetPodNADToNetworkMapping(pod, a.netInfo)
	if err != nil {
		return fmt.Errorf("failed to get NAD to network mapping: %w", err)
	}

	// nothing to do if not on this network
	// Note: we are not considering a hotplug scenario where we would have to
	// release IPs if the pod was unplugged from the network
	if !onNetwork {
		return nil
	}

	// reconcile for each NAD
	for nadName, network := range networkMap {
		err = a.reconcileForNAD(old, new, nadName, network, releaseFromAllocator)
		if err != nil {
			return err
		}
	}

	return nil
}

func (a *PodAllocator) reconcileForNAD(old, new *corev1.Pod, nad string, network *nettypes.NetworkSelectionElement, releaseIPsFromAllocator bool) error {
	var pod *corev1.Pod
	if old != nil {
		pod = old
	}
	if new != nil {
		pod = new
	}
	podDeleted := new == nil
	podCompleted := util.PodCompleted(pod)

	if podCompleted || podDeleted {
		return a.releasePodOnNAD(pod, nad, network, podDeleted, releaseIPsFromAllocator)
	}

	return a.allocatePodOnNAD(pod, nad, network)
}

func (a *PodAllocator) releasePodOnNAD(pod *corev1.Pod, nad string, networkSelectionElement *nettypes.NetworkSelectionElement, podDeleted, releaseFromAllocator bool) error {
	podAnnotation, _ := util.UnmarshalPodAnnotation(pod.Annotations, nad)
	if podAnnotation == nil {
		// track release pods even if they have no annotation in case a user
		// might have removed it manually
		podAnnotation = &util.PodAnnotation{}
	}

	uid := string(pod.UID)

	hasIPAM := util.DoesNetworkRequireIPAM(a.netInfo)
	hasIDAllocation := util.DoesNetworkRequireTunnelIDs(a.netInfo)

	hasPersistentIPs := networkSelectionElement.IPAMClaimReference != ""
	if hasPersistentIPs {
		_, err := a.watchFactory.GetPersistentIPs(pod.Namespace, networkSelectionElement.IPAMClaimReference)
		hasPersistentIPs = err == nil
	}
	if !hasIPAM && !hasIDAllocation {
		// we only take care of IP and tunnel ID allocation, if neither were
		// allocated we have nothing to do
		return nil
	}

	// do not release from the allocators if not flaged to do so or if they
	// were already previosuly released
	doRelease := releaseFromAllocator && !a.isPodReleased(nad, uid)
	doReleaseIDs := doRelease && hasIDAllocation
	doReleaseIPs := doRelease && hasIPAM && !hasPersistentIPs

	if doReleaseIDs {
		name := podIdAllocationName(nad, uid)
		a.idAllocator.ReleaseID(name)
		klog.V(5).Infof("Released ID %d", podAnnotation.TunnelID)
	}

	if doReleaseIPs {
		err := a.ipAllocator.ReleaseIPs(a.netInfo.GetNetworkName(), podAnnotation.IPs)
		if err != nil {
			return fmt.Errorf("failed to release ips %v for pod %s/%s and nad %s: %w",
				util.StringSlice(podAnnotation.IPs),
				pod.Name,
				pod.Namespace,
				nad,
				err,
			)
		}
		klog.V(5).Infof("Released IPs %v", util.StringSlice(podAnnotation.IPs))
	}

	if podDeleted {
		a.deleteReleasedPod(nad, string(pod.UID))
	} else {
		a.addReleasedPod(nad, string(pod.UID))
	}

	return nil
}

func (a *PodAllocator) allocatePodOnNAD(pod *corev1.Pod, nad string, network *nettypes.NetworkSelectionElement) error {
	var (
		ipAllocator            subnet.NamedAllocator
		persistentIPsAllocator *persistentips.Allocator
	)
	if util.DoesNetworkRequireIPAM(a.netInfo) {
		ipAllocator = a.ipAllocator.ForSubnet(a.netInfo.GetNetworkName())
		persistentIPsAllocator = persistentips.NewPersistentIPsAllocator(a.kube, ipAllocator)
	}

	var idAllocator id.NamedAllocator
	if util.DoesNetworkRequireTunnelIDs(a.netInfo) {
		name := podIdAllocationName(nad, string(pod.UID))
		idAllocator = a.idAllocator.ForName(name)
	}

	var ipamClaim *persistentipsapi.IPAMClaim
	if util.DoesNetworkRequireIPAM(a.netInfo) {
		klog.Infof("Allocate IPAMClaim for pod NAD: %q", nad)
		var err error
		ipamClaim, err = a.findIPAMClaim(pod, network)
		if err != nil {
			return err
		}
	}

	const dontReallocate = false // don't reallocate to new IPs if currently annotated IPs fail to allocate
	updatedPod, podAnnotation, err := a.podAnnotationAllocator.AllocatePodAnnotationWithTunnelID(
		ipAllocator,
		idAllocator,
		pod,
		network,
		ipamClaim,
		dontReallocate,
	)

	if err != nil {
		return err
	}

	if ipamClaim != nil && persistentIPsAllocator != nil {
		if err := persistentIPsAllocator.Reconcile(ipamClaim, util.StringSlice(podAnnotation.IPs)); err != nil {
			return err
		}
	}

	if updatedPod != nil {
		klog.V(5).Infof(
			"Allocated IP addresses %v, mac address %s, gateways %v, routes %s and tunnel id %d for pod %s/%s on nad %s",
			util.StringSlice(podAnnotation.IPs),
			podAnnotation.MAC,
			util.StringSlice(podAnnotation.Gateways),
			util.StringSlice(podAnnotation.Routes),
			podAnnotation.TunnelID,
			pod.Namespace,
			pod.Name,
			nad,
		)
	}

	return err
}

func (a *PodAllocator) findIPAMClaim(pod *corev1.Pod, network *nettypes.NetworkSelectionElement) (*persistentipsapi.IPAMClaim, error) {
	if network.IPAMClaimReference != "" {
		ipamClaimKey := network.IPAMClaimReference

		klog.V(5).Infof("IPAMClaim key: %s", ipamClaimKey)
		claim, err := a.watchFactory.GetPersistentIPs(pod.Namespace, ipamClaimKey)
		if err != nil {
			return nil, fmt.Errorf("failed to get ipamLease %q", ipamClaimKey)
		}

		return claim, nil
	}
	return nil, nil
}

func (a *PodAllocator) addReleasedPod(nad, uid string) {
	a.releasedPodsMutex.Lock()
	defer a.releasedPodsMutex.Unlock()
	releasedPods := a.releasedPods[nad]
	if releasedPods == nil {
		a.releasedPods[nad] = sets.New(uid)
		return
	}
	releasedPods.Insert(uid)
}

func (a *PodAllocator) deleteReleasedPod(nad, uid string) {
	a.releasedPodsMutex.Lock()
	defer a.releasedPodsMutex.Unlock()
	releasedPods := a.releasedPods[nad]
	if releasedPods != nil {
		releasedPods.Delete(uid)
		if releasedPods.Len() == 0 {
			delete(a.releasedPods, nad)
		}
	}
}

func (a *PodAllocator) isPodReleased(nad, uid string) bool {
	a.releasedPodsMutex.Lock()
	defer a.releasedPodsMutex.Unlock()
	releasedPods := a.releasedPods[nad]
	if releasedPods != nil {
		return releasedPods.Has(uid)
	}
	return false
}

func podIdAllocationName(nad, uid string) string {
	return fmt.Sprintf("%s/%s", nad, uid)
}
