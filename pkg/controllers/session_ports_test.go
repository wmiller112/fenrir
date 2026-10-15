package controllers

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	v1alpha1types "games-on-whales.github.io/direwolf/pkg/api/v1alpha1"
	generatedclient "games-on-whales.github.io/direwolf/pkg/generated/clientset/versioned/fake"
	generatedinformers "games-on-whales.github.io/direwolf/pkg/generated/informers/externalversions"
	"games-on-whales.github.io/direwolf/pkg/generic"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	k8sfake "k8s.io/client-go/kubernetes/fake"
)

func testSession(name, user, game string, ports v1alpha1types.SessionPorts) *v1alpha1types.Session {
	return &v1alpha1types.Session{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "direwolf", UID: types.UID("uid-" + name)},
		Spec: v1alpha1types.SessionSpec{
			UserReference: v1alpha1types.UserReference{Name: user},
			GameReference: v1alpha1types.GameReference{Name: game},
		},
		Status: v1alpha1types.SessionStatus{Ports: ports},
	}
}

// newPortsTestController builds a controller whose session informer holds the
// given (already allocated) sessions, like the operator sees after a restart.
func newPortsTestController(t *testing.T, existing ...runtime.Object) *SessionController {
	t.Helper()
	client := generatedclient.NewSimpleClientset(existing...)
	factory := generatedinformers.NewSharedInformerFactory(client, 0)
	informer := factory.Direwolf().V1alpha1().Sessions().Informer()

	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	factory.Start(stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory.WaitForCacheSync(ctx.Done())

	return &SessionController{
		SessionInformer: generic.NewInformer[*v1alpha1types.Session](informer),
		trackedSessions: make(map[userGame]sets.Set[string]),
		trackedGames:    make(map[string]userGame),
		portSlots:       make(map[userGame]int32),
	}
}

func allocate(t *testing.T, c *SessionController, s *v1alpha1types.Session) v1alpha1types.SessionPorts {
	t.Helper()
	if err := c.allocatePorts(context.Background(), s); err != nil {
		t.Fatalf("allocatePorts(%s): %v", s.Name, err)
	}
	return s.Status.Ports
}

func TestPortSlots(t *testing.T) {
	if got := portsForSlot(0); got != basePorts {
		t.Errorf("slot 0 = %+v, want Wolf's defaults %+v", got, basePorts)
	}

	want := v1alpha1types.SessionPorts{RTSP: 49010, Control: 48999, VideoRTP: 49100, AudioRTP: 49200}
	if got := portsForSlot(1); got != want {
		t.Errorf("slot 1 = %+v, want %+v", got, want)
	}

	// Blocks of different slots never share a port, wolf-agent's included,
	// since any overlap stops MetalLB sharing the LoadBalancer IP
	seen := map[int32]int32{}
	for slot := int32(0); slot < maxPortSlots; slot++ {
		p := portsForSlot(slot)
		for _, port := range []int32{p.RTSP, p.Control, p.VideoRTP, p.AudioRTP, wolfAgentServicePort(p)} {
			if other, dup := seen[port]; dup {
				t.Errorf("port %d used by slots %d and %d", port, other, slot)
			}
			seen[port] = slot
		}
		if got, ok := portSlotOf(p); !ok || got != slot {
			t.Errorf("portSlotOf(slot %d ports) = %d, %v", slot, got, ok)
		}
	}

	for _, ports := range []v1alpha1types.SessionPorts{
		{},
		{RTSP: 48011, Control: 48000, VideoRTP: 48101, AudioRTP: 48201},
		{RTSP: 49010, Control: 47999, VideoRTP: 48100, AudioRTP: 48200},
	} {
		if slot, ok := portSlotOf(ports); ok {
			t.Errorf("portSlotOf(%+v) = slot %d, want no slot", ports, slot)
		}
	}
}

func TestAllocatePortsPerUserGame(t *testing.T) {
	c := newPortsTestController(t)

	alex := allocate(t, c, testSession("alex-1", "alex", "retroarch", v1alpha1types.SessionPorts{}))
	player2 := allocate(t, c, testSession("player2-1", "player2", "retroarch", v1alpha1types.SessionPorts{}))
	if alex != portsForSlot(0) {
		t.Errorf("alex got %+v, want slot 0", alex)
	}
	// alex's status hasn't been written back yet, so this relies on the
	// in-memory reservation
	if player2 != portsForSlot(1) {
		t.Errorf("player2 got %+v, want slot 1", player2)
	}

	// Another session of alex's game shares the pod, so it shares the ports
	if got := allocate(t, c, testSession("alex-2", "alex", "retroarch", v1alpha1types.SessionPorts{})); got != alex {
		t.Errorf("alex's second session got %+v, want alex's %+v", got, alex)
	}

	// Same user, different game is a different pod
	if got := allocate(t, c, testSession("alex-steam", "alex", "steam", v1alpha1types.SessionPorts{})); got != portsForSlot(2) {
		t.Errorf("alex's steam session got %+v, want slot 2", got)
	}

	// Ports already in a session's status are kept
	kept := testSession("player3-1", "player3", "retroarch", portsForSlot(7))
	if got := allocate(t, c, kept); got != portsForSlot(7) {
		t.Errorf("existing ports changed to %+v", got)
	}

	// Once alex's game has no sessions, its slot is free again
	c.releasePorts(userGame{User: "alex", Game: "retroarch"})
	if got := allocate(t, c, testSession("player4-1", "player4", "retroarch", v1alpha1types.SessionPorts{})); got != portsForSlot(0) {
		t.Errorf("player4 got %+v, want alex's released slot 0", got)
	}
}

func TestAllocatePortsAfterRestart(t *testing.T) {
	// The operator restarted: sessions keep their ports in status, but the
	// in-memory reservations are gone
	c := newPortsTestController(t,
		testSession("alex-old", "alex", "retroarch", portsForSlot(0)),
		testSession("player2-old", "player2", "retroarch", portsForSlot(1)),
	)

	if got := allocate(t, c, testSession("player2-new", "player2", "retroarch", v1alpha1types.SessionPorts{})); got != portsForSlot(1) {
		t.Errorf("player2's new session got %+v, want its existing slot 1", got)
	}
	if got := allocate(t, c, testSession("player3-new", "player3", "retroarch", v1alpha1types.SessionPorts{})); got != portsForSlot(2) {
		t.Errorf("player3 got %+v, want first free slot 2", got)
	}
}

func TestAllocatePortsConcurrent(t *testing.T) {
	c := newPortsTestController(t)

	const users = 8
	results := make([]v1alpha1types.SessionPorts, users)
	var wg sync.WaitGroup
	for i := range users {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s := testSession(fmt.Sprintf("s%d", i), fmt.Sprintf("user%d", i), "retroarch", v1alpha1types.SessionPorts{})
			if err := c.allocatePorts(context.Background(), s); err != nil {
				t.Errorf("allocatePorts: %v", err)
			}
			results[i] = s.Status.Ports
		}()
	}
	wg.Wait()

	seen := map[int32]bool{}
	for i, p := range results {
		if seen[p.RTSP] {
			t.Errorf("user%d got RTSP port %d, already given to another user", i, p.RTSP)
		}
		seen[p.RTSP] = true
	}
}

func TestAllocatePortsExhausted(t *testing.T) {
	c := newPortsTestController(t)
	for i := range maxPortSlots {
		allocate(t, c, testSession(fmt.Sprintf("s%d", i), fmt.Sprintf("user%d", i), "retroarch", v1alpha1types.SessionPorts{}))
	}
	err := c.allocatePorts(context.Background(), testSession("one-more", "one-more", "retroarch", v1alpha1types.SessionPorts{}))
	if err == nil {
		t.Fatal("allocated ports beyond the last slot")
	}
}

func TestReconcileServiceKeepsOtherUsersStreams(t *testing.T) {
	ctx := context.Background()
	service := func(name, user string) *corev1.Service {
		return &corev1.Service{ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "direwolf",
			Labels:    map[string]string{"app": "direwolf-worker", "direwolf/app": "retroarch", "direwolf/user": user},
		}}
	}
	k8s := k8sfake.NewSimpleClientset(
		service("alex-old-rtp", "alex"),
		service("player2-1-rtp", "player2"),
		service("alex-new-rtp", "alex"),
	)
	c := &SessionController{K8sClient: k8s}

	s := testSession("alex-new", "alex", "retroarch", portsForSlot(0))
	meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: "PortsAllocated", Status: metav1.ConditionTrue, Reason: "Test"})
	if err := c.reconcileService(ctx, s); err != nil {
		t.Fatalf("reconcileService: %v", err)
	}

	list, err := k8s.CoreV1().Services("direwolf").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	got := sets.New[string]()
	for _, svc := range list.Items {
		got.Insert(svc.Name)
	}
	if want := sets.New("alex-new-rtp", "player2-1-rtp"); !got.Equal(want) {
		t.Errorf("services left = %v, want %v (player2's stream kept, alex's old one removed)", sets.List(got), sets.List(want))
	}
}

func TestStreamServicesHaveNoOverlappingPorts(t *testing.T) {
	ctx := context.Background()
	// The fake clientset only applies to existing objects
	k8s := k8sfake.NewSimpleClientset(
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "alex-1-rtp", Namespace: "direwolf"}},
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "player2-1-rtp", Namespace: "direwolf"}},
	)
	c := &SessionController{K8sClient: k8s}

	for i, user := range []string{"alex", "player2"} {
		s := testSession(user+"-1", user, "retroarch", portsForSlot(int32(i)))
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: "PortsAllocated", Status: metav1.ConditionTrue, Reason: "Test"})
		if err := c.reconcileService(ctx, s); err != nil {
			t.Fatalf("reconcileService(%s): %v", user, err)
		}
	}

	list, err := k8s.CoreV1().Services("direwolf").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 2 {
		t.Fatalf("got %d services, want one per user", len(list.Items))
	}

	owner := map[string]string{}
	for _, svc := range list.Items {
		for _, port := range svc.Spec.Ports {
			key := fmt.Sprintf("%d/%s", port.Port, port.Protocol)
			if other, dup := owner[key]; dup {
				t.Errorf("port %s is on both %s and %s, so they can't share a LoadBalancer IP", key, other, svc.Name)
			}
			owner[key] = svc.Name

			if port.Name == "wa" && port.TargetPort.IntValue() != wolfAgentPort {
				t.Errorf("%s: wolf-agent targets port %s, want %d", svc.Name, port.TargetPort.String(), wolfAgentPort)
			}
		}
	}
}
