package controllers

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/r3labs/sse/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	sigsyaml "sigs.k8s.io/yaml"

	v1alpha1types "games-on-whales.github.io/direwolf/pkg/api/v1alpha1"
	generatedclient "games-on-whales.github.io/direwolf/pkg/generated/clientset/versioned/fake"
	generatedinformers "games-on-whales.github.io/direwolf/pkg/generated/informers/externalversions"
	"games-on-whales.github.io/direwolf/pkg/generic"
	"games-on-whales.github.io/direwolf/pkg/wolfapi"
)

func testApp(name string, coop *v1alpha1types.CoopConfig) *v1alpha1types.App {
	return &v1alpha1types.App{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "direwolf"},
		Spec:       v1alpha1types.AppSpec{Title: name, Coop: coop},
	}
}

// newCoopTestController builds a controller whose session and app informers
// hold the given objects.
func newCoopTestController(t *testing.T, existing ...runtime.Object) *SessionController {
	t.Helper()
	client := generatedclient.NewSimpleClientset(existing...)
	factory := generatedinformers.NewSharedInformerFactory(client, 0)
	sessions := factory.Direwolf().V1alpha1().Sessions().Informer()
	apps := factory.Direwolf().V1alpha1().Apps().Informer()

	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	factory.Start(stop)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	factory.WaitForCacheSync(ctx.Done())

	return &SessionController{
		SessionInformer: generic.NewInformer[*v1alpha1types.Session](sessions),
		AppInformer:     generic.NewInformer[*v1alpha1types.App](apps),
		trackedSessions: make(map[userGame]sets.Set[string]),
		trackedGames:    make(map[string]userGame),
		portSlots:       make(map[userGame]int32),
	}
}

func TestCoopSessionsSharePod(t *testing.T) {
	alexPS2 := testSession("alex-1", "alex", "ps2", v1alpha1types.SessionPorts{})
	player2PS2 := testSession("player2-1", "player2", "ps2", v1alpha1types.SessionPorts{})
	alexSteam := testSession("alex-2", "alex", "steam", v1alpha1types.SessionPorts{})
	c := newCoopTestController(t,
		testApp("ps2", &v1alpha1types.CoopConfig{}),
		testApp("steam", nil),
		alexPS2, player2PS2, alexSteam,
	)

	if got := c.deploymentName(alexPS2); got != "coop-ps2" {
		t.Errorf("co-op deployment = %q, want coop-ps2", got)
	}
	if got := c.deploymentName(player2PS2); got != "coop-ps2" {
		t.Errorf("player2's co-op deployment = %q, want the shared coop-ps2", got)
	}
	if got := c.deploymentName(alexSteam); got != "alex-steam" {
		t.Errorf("steam deployment = %q, want alex-steam", got)
	}

	wantLabels := map[string]string{"direwolf/app": "ps2", "direwolf/user": ""}
	for k, v := range wantLabels {
		if got, ok := c.podLabels(player2PS2)[k]; !ok || got != v {
			t.Errorf("co-op pod label %s = %q (present %v), want %q", k, got, ok, v)
		}
	}

	// Both players stream from the same pod, so they get the same ports
	alexPorts := allocate(t, c, alexPS2)
	if got := allocate(t, c, player2PS2); got != alexPorts {
		t.Errorf("player2 got ports %+v, want alex's %+v", got, alexPorts)
	}
	if got := allocate(t, c, alexSteam); got == alexPorts {
		t.Errorf("alex's steam session shares the co-op ports %+v", got)
	}
}

func TestSessionOwners(t *testing.T) {
	alex := testSession("alex-1", "alex", "ps2", v1alpha1types.SessionPorts{})
	player2 := testSession("player2-1", "player2", "ps2", v1alpha1types.SessionPorts{})
	c := newCoopTestController(t, testApp("ps2", &v1alpha1types.CoopConfig{}), alex, player2)
	c.trackedSessions[c.podKey(alex)] = sets.New(alex.Name, player2.Name)

	owners := c.sessionOwners(alex)
	if len(owners) != 2 {
		t.Fatalf("got %d owners, want both sessions: %+v", len(owners), owners)
	}
	for i, want := range []*v1alpha1types.Session{alex, player2} {
		if owners[i].Name != want.Name || owners[i].UID != want.UID {
			t.Errorf("owner %d = %s (%s), want %s (%s)", i, owners[i].Name, owners[i].UID, want.Name, want.UID)
		}
		// An object may only have one controller
		if owners[i].Controller != nil {
			t.Errorf("owner %s is set as controller", owners[i].Name)
		}
	}
}

type fakeWolf struct {
	wolfapi.Client
	lobbies []wolfapi.Lobby
	created []wolfapi.CreateLobbyRequest
	joined  []string
}

func (f *fakeWolf) ListLobbies(context.Context) ([]wolfapi.Lobby, error) {
	return f.lobbies, nil
}

func (f *fakeWolf) CreateLobby(_ context.Context, req wolfapi.CreateLobbyRequest) (string, error) {
	f.created = append(f.created, req)
	id := "lobby-" + req.Name
	f.lobbies = append(f.lobbies, wolfapi.Lobby{ID: id, Name: req.Name})
	return id, nil
}

func (f *fakeWolf) JoinLobby(_ context.Context, lobbyID, sessionID string) error {
	f.joined = append(f.joined, lobbyID+"/"+sessionID)
	for i := range f.lobbies {
		if f.lobbies[i].ID == lobbyID {
			f.lobbies[i].ConnectedSessions = append(f.lobbies[i].ConnectedSessions, sessionID)
		}
	}
	return nil
}

func (f *fakeWolf) SubscribeToEvents(context.Context) (<-chan *sse.Event, error) {
	return nil, nil
}

func TestReconcileLobby(t *testing.T) {
	ctx := context.Background()
	app := testApp("ps2", &v1alpha1types.CoopConfig{})
	c := newCoopTestController(t)
	wolf := &fakeWolf{lobbies: []wolfapi.Lobby{{ID: "other", Name: "direwolf-steam"}}}

	alex := testSession("alex-1", "alex", "ps2", v1alpha1types.SessionPorts{})
	alex.Spec.Config = v1alpha1types.SessionInfo{VideoWidth: 1280, VideoHeight: 720, VideoRefreshRate: 60}
	alex.Status.WolfSessionID = "111"

	// The first player creates the lobby and joins it
	if err := c.reconcileLobby(ctx, wolf, alex, app, app.Spec.Coop); err != nil {
		t.Fatalf("reconcileLobby(alex): %v", err)
	}
	if len(wolf.created) != 1 {
		t.Fatalf("created %d lobbies, want 1", len(wolf.created))
	}
	created := wolf.created[0]
	if created.Name != "direwolf-ps2" || !created.MultiUser || !created.StopWhenEveryoneLeaves {
		t.Errorf("unexpected lobby %+v", created)
	}
	if v := created.VideoSettings; v.Width != 1280 || v.Height != 720 || v.RefreshRate != 60 ||
		v.VideoProducerBufferCaps != "video/x-raw" || v.WaylandRenderNode != "/dev/dri/renderD128" {
		t.Errorf("unexpected lobby video settings %+v", v)
	}
	if created.Runner.Type != "process" || !strings.Contains(created.Runner.RunCmd, coopWaylandDisplay) {
		t.Errorf("unexpected lobby runner %+v", created.Runner)
	}

	// The second player joins the running lobby
	player2 := testSession("player2-1", "player2", "ps2", v1alpha1types.SessionPorts{})
	player2.Status.WolfSessionID = "222"
	if err := c.reconcileLobby(ctx, wolf, player2, app, app.Spec.Coop); err != nil {
		t.Fatalf("reconcileLobby(player2): %v", err)
	}
	// Reconciling again doesn't rejoin
	if err := c.reconcileLobby(ctx, wolf, alex, app, app.Spec.Coop); err != nil {
		t.Fatalf("reconcileLobby(alex) again: %v", err)
	}

	if len(wolf.created) != 1 {
		t.Errorf("created %d lobbies, want 1", len(wolf.created))
	}
	want := []string{"lobby-direwolf-ps2/111", "lobby-direwolf-ps2/222"}
	if strings.Join(wolf.joined, ",") != strings.Join(want, ",") {
		t.Errorf("joined %v, want %v", wolf.joined, want)
	}

	// Without a Wolf session there is nothing to join yet
	pending := testSession("player3-1", "player3", "ps2", v1alpha1types.SessionPorts{})
	if err := c.reconcileLobby(ctx, wolf, pending, app, app.Spec.Coop); err == nil {
		t.Error("reconcileLobby without a wolf session succeeded")
	}
}

func TestReconcileLobbyCustomCaps(t *testing.T) {
	app := testApp("ps2", &v1alpha1types.CoopConfig{VideoProducerBufferCaps: "video/x-raw(memory:CUDAMemory)"})
	app.Spec.WolfConfig.RuntimeVariables = &v1alpha1types.RuntimeWolfVariables{RenderNode: "/dev/dri/renderD129"}
	c := newCoopTestController(t)
	wolf := &fakeWolf{}

	s := testSession("alex-1", "alex", "ps2", v1alpha1types.SessionPorts{})
	s.Status.WolfSessionID = "1"
	if err := c.reconcileLobby(context.Background(), wolf, s, app, app.Spec.Coop); err != nil {
		t.Fatalf("reconcileLobby: %v", err)
	}

	v := wolf.created[0].VideoSettings
	if v.VideoProducerBufferCaps != "video/x-raw(memory:CUDAMemory)" {
		t.Errorf("caps = %q", v.VideoProducerBufferCaps)
	}
	if v.WaylandRenderNode != "/dev/dri/renderD129" || v.RunnerRenderNode != "/dev/dri/renderD129" {
		t.Errorf("render nodes = %q, %q", v.WaylandRenderNode, v.RunnerRenderNode)
	}
}

func TestCoopSessionsShareService(t *testing.T) {
	ctx := context.Background()
	alex := testSession("alex-1", "alex", "ps2", portsForSlot(0))
	player2 := testSession("player2-1", "player2", "ps2", portsForSlot(0))
	c := newCoopTestController(t, testApp("ps2", &v1alpha1types.CoopConfig{}), alex, player2)
	c.trackedSessions[c.podKey(alex)] = sets.New(alex.Name, player2.Name)
	// The fake clientset only applies to existing objects
	c.K8sClient = k8sfake.NewSimpleClientset(
		&corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: "coop-ps2-rtp", Namespace: "direwolf"}},
	)

	for _, s := range []*v1alpha1types.Session{alex, player2} {
		meta.SetStatusCondition(&s.Status.Conditions, metav1.Condition{Type: "PortsAllocated", Status: metav1.ConditionTrue, Reason: "Test"})
		if err := c.reconcileService(ctx, s); err != nil {
			t.Fatalf("reconcileService(%s): %v", s.Name, err)
		}
		if s.Status.ServiceName != "coop-ps2-rtp" {
			t.Errorf("%s uses service %q, want the shared coop-ps2-rtp", s.Name, s.Status.ServiceName)
		}
	}

	list, err := c.K8sClient.CoreV1().Services("direwolf").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 1 {
		t.Fatalf("got %d services, want one shared by both players", len(list.Items))
	}
	svc := list.Items[0]
	if len(svc.OwnerReferences) != 2 {
		t.Errorf("service owners = %+v, want both sessions", svc.OwnerReferences)
	}
	if svc.Spec.Selector["direwolf/user"] != "" || svc.Spec.Selector["direwolf/app"] != "ps2" {
		t.Errorf("service selects %v, want the co-op pod", svc.Spec.Selector)
	}
}

func TestPS2CoopExample(t *testing.T) {
	data, err := os.ReadFile("../../examples/ps2-coop.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var app v1alpha1types.App
	if err := sigsyaml.UnmarshalStrict(data, &app); err != nil {
		t.Fatalf("examples/ps2-coop.yaml doesn't match the App type: %v", err)
	}
	if app.Spec.Coop == nil {
		t.Error("example isn't a co-op app")
	}
	if len(app.Spec.AppAssetWebP) == 0 || app.Spec.Template == nil {
		t.Error("example is missing its icon or pod template")
	}
}
