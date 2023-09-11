package persistentips

import (
	"context"
	"testing"

	. "github.com/onsi/ginkgo"
	"github.com/onsi/ginkgo/extensions/table"
	. "github.com/onsi/gomega"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ipamclaimsapi "github.com/maiqueb/persistentips/pkg/crd/persistentip/v1alpha1"
	fakeipamclaimclient "github.com/maiqueb/persistentips/pkg/crd/persistentip/v1alpha1/apis/clientset/versioned/fake"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/allocator/ip/subnet"
	ovnkclient "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	ovntest "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/testing"
)

func TestPersistenIPAllocator(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Persistent IP allocator Suite")
}

var _ = Describe("Persistent IP allocator operations", func() {
	const (
		namespace = "ns1"
		claimName = "claim1"
	)
	var (
		persistentIPAllocator *Allocator
		ovnkapiclient         *ovnkclient.KubeOVN
	)

	Context("an existing, but empty IPAMClaim", func() {
		BeforeEach(func() {
			ipAllocator := subnet.NewAllocator()
			ovnkapiclient = &ovnkclient.KubeOVN{
				Kube: ovnkclient.Kube{},
				PersistentIPsClient: fakeipamclaimclient.NewSimpleClientset(
					emptyDummyIPAMClaim(namespace, claimName),
				),
			}
			Expect(ipAllocator.AddOrUpdateSubnet("", ovntest.MustParseIPNets("192.168.200.0/24", "fd10::/64"))).To(Succeed())
			persistentIPAllocator = NewPersistentIPsAllocator(ovnkapiclient, ipAllocator.ForSubnet(""))
		})

		table.DescribeTable("reconciling IPAMClaims is successful when provided with", func(ipamClaim *ipamclaimsapi.IPAMClaim, ips ...string) {
			Expect(persistentIPAllocator.Reconcile(ipamClaim, ips)).To(Succeed())
			updatedIPAMClaim, err := ovnkapiclient.PersistentIPsClient.K8sV1alpha1().IPAMClaims(namespace).Get(context.Background(), claimName, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedIPAMClaim.Status.IPs).To(ConsistOf(ips))
		},
			table.Entry("no IP addresses to persist", emptyDummyIPAMClaim(namespace, claimName)),
			table.Entry("an IP addresses to persist", emptyDummyIPAMClaim(namespace, claimName), "192.168.200.20/24"),
		)

		table.DescribeTable("syncing the IP allocator from the IPAMClaims is successful when provided with", func(ipamClaims ...interface{}) {
			Expect(persistentIPAllocator.Sync(ipamClaims)).To(Succeed())
		},
			table.Entry("no objects to sync with"),
			table.Entry("an IPAMClaim without persisted IPs", emptyDummyIPAMClaim(namespace, claimName)),
			table.Entry("an IPAMClaim with persisted IPs", ipamClaimWithIPs("192.168.200.2/24", "fd10::1/64")),
		)
	})

	When("reconciling an IPAMClaim already featuring IPs", func() {
		const originalIPAMClaimIP = "192.168.200.2/24"

		BeforeEach(func() {
			ipAllocator := subnet.NewAllocator()
			ovnkapiclient = &ovnkclient.KubeOVN{
				Kube: ovnkclient.Kube{},
				PersistentIPsClient: fakeipamclaimclient.NewSimpleClientset(
					ipamClaimWithIPs(namespace, claimName, originalIPAMClaimIP),
				),
			}
			Expect(ipAllocator.AddOrUpdateSubnet("", ovntest.MustParseIPNets("192.168.200.0/24", "fd10::/64"))).To(Succeed())
			persistentIPAllocator = NewPersistentIPsAllocator(ovnkapiclient, ipAllocator.ForSubnet(""))
		})

		It("the IPAMClaim is *not* updated", func() {
			Expect(persistentIPAllocator.Reconcile(
				ipamClaimWithIPs(namespace, claimName, originalIPAMClaimIP),
				[]string{"192.168.200.0/24", "fd10::/64"},
			)).To(Succeed())

			updatedIPAMClaim, err := ovnkapiclient.PersistentIPsClient.K8sV1alpha1().IPAMClaims(namespace).Get(context.Background(), claimName, metav1.GetOptions{})
			Expect(err).NotTo(HaveOccurred())
			Expect(updatedIPAMClaim.Status.IPs).To(ConsistOf(originalIPAMClaimIP))
		})
	})

	Context("an IPAllocator having already allocated some addresses", func() {
		var ipAllocator subnet.NamedAllocator
		BeforeEach(func() {
			ipAllocationMachine := subnet.NewAllocator()
			Expect(ipAllocationMachine.AddOrUpdateSubnet("", ovntest.MustParseIPNets("192.168.200.0/24", "fd10::/64"))).To(Succeed())
			Expect(ipAllocationMachine.AllocateIPs("", ovntest.MustParseIPNets("192.168.200.2/24", "fd10::1/64"))).To(Succeed())
			ipAllocator = ipAllocationMachine.ForSubnet("")
		})

		It("successfully handles being requested the same IPs again", func() {
			persistentIPAllocator := NewPersistentIPsAllocator(ovnkapiclient, ipAllocator)
			Expect(
				persistentIPAllocator.Sync(
					[]interface{}{ipamClaimWithIPs(
						namespace,
						claimName,
						"192.168.200.2/24",
						"fd10::1/64",
					)}),
			).To(Succeed())
		})
	})
})

func emptyDummyIPAMClaim(namespace string, claimName string) *ipamclaimsapi.IPAMClaim {
	return &ipamclaimsapi.IPAMClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: namespace,
		},
		Spec: ipamclaimsapi.IPAMClaimSpec{},
	}
}

func ipamClaimWithIPs(namespace string, claimName string, ips ...string) *ipamclaimsapi.IPAMClaim {
	return &ipamclaimsapi.IPAMClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      claimName,
			Namespace: namespace,
		},
		Spec: ipamclaimsapi.IPAMClaimSpec{},
		Status: ipamclaimsapi.IPAMClaimStatus{
			IPs: ips,
		},
	}
}
