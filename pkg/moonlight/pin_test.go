package moonlight

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/cache"

	v1alpha1types "games-on-whales.github.io/direwolf/pkg/api/v1alpha1"
	"games-on-whales.github.io/direwolf/pkg/generic"
)

func newPinTestServer(t *testing.T, usernames ...string) *RESTServer {
	t.Helper()
	indexer := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{
		cache.NamespaceIndex: cache.MetaNamespaceIndexFunc,
	})
	for _, name := range usernames {
		if err := indexer.Add(&v1alpha1types.User{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "direwolf"},
		}); err != nil {
			t.Fatal(err)
		}
	}
	// A user in another namespace must never be offered
	if err := indexer.Add(&v1alpha1types.User{
		ObjectMeta: metav1.ObjectMeta{Name: "elsewhere", Namespace: "other"},
	}); err != nil {
		t.Fatal(err)
	}

	return &RESTServer{
		manager:    &PairingManager{},
		UserLister: generic.NewLister[*v1alpha1types.User](indexer).Namespaced("direwolf"),
	}
}

func addPendingPin(s *RESTServer, secret string, deviceName string, started time.Time) chan pinSubmission {
	submissions := make(chan pinSubmission, 1)
	s.manager.PendingPins.Store(secret, &pendingPin{
		Submissions: submissions,
		DeviceName:  deviceName,
		ClientIP:    "10.0.0.2",
		Started:     started,
	})
	return submissions
}

func TestPinPendingHandler(t *testing.T) {
	s := newPinTestServer(t, "alex")

	get := func() []PendingPairing {
		t.Helper()
		rec := httptest.NewRecorder()
		s.pinPendingHandler(rec, httptest.NewRequest("GET", "/pin/pending", nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("got status %d: %s", rec.Code, rec.Body.String())
		}
		var body struct {
			Pending []PendingPairing `json:"pending"`
		}
		if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Pending == nil {
			t.Fatal("pending should be an empty list, not null")
		}
		return body.Pending
	}

	if got := get(); len(got) != 0 {
		t.Fatalf("got %d pending pairings, want none", len(got))
	}

	start := time.Date(2026, 10, 14, 22, 0, 0, 0, time.UTC)
	addPendingPin(s, "newer", "deck", start.Add(time.Minute))
	addPendingPin(s, "older", "roth", start)

	want := []PendingPairing{
		{Secret: "older", DeviceName: "roth", ClientIP: "10.0.0.2", Started: start},
		{Secret: "newer", DeviceName: "deck", ClientIP: "10.0.0.2", Started: start.Add(time.Minute)},
	}
	if diff := cmp.Diff(want, get()); diff != "" {
		t.Errorf("pending mismatch, want oldest first (-want +got):\n%s", diff)
	}
}

func TestPostPinWithoutSecret(t *testing.T) {
	tests := []struct {
		name       string
		pending    []string
		wantCode   int
		wantSecret string
	}{
		{name: "single pending pairing is picked", pending: []string{"a"}, wantCode: 200, wantSecret: "a"},
		{name: "nothing pending", wantCode: 400},
		{name: "must choose with several pending", pending: []string{"a", "b"}, wantCode: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newPinTestServer(t, "alex")
			channels := map[string]chan pinSubmission{}
			for _, secret := range tt.pending {
				channels[secret] = addPendingPin(s, secret, "roth", time.Now())
			}

			rec := httptest.NewRecorder()
			s.pinHandler(rec, httptest.NewRequest("POST", "/pin/", strings.NewReader(`{"pin":"1234"}`)))

			if rec.Code != tt.wantCode {
				t.Fatalf("got status %d (%s), want %d", rec.Code, rec.Body.String(), tt.wantCode)
			}
			for secret, ch := range channels {
				select {
				case got := <-ch:
					if secret != tt.wantSecret {
						t.Errorf("PIN went to pairing %q, want %q", secret, tt.wantSecret)
					}
					if diff := cmp.Diff(pinSubmission{Pin: "1234", Username: "alex"}, got); diff != "" {
						t.Errorf("submission mismatch (-want +got):\n%s", diff)
					}
				default:
					if secret == tt.wantSecret {
						t.Errorf("PIN never reached pairing %q", secret)
					}
				}
			}
		})
	}
}

func TestResolvePinUser(t *testing.T) {
	tests := []struct {
		name      string
		users     []string
		requested string
		want      string
		wantErr   string
	}{
		{name: "single user is picked when none requested", users: []string{"alex"}, want: "alex"},
		{name: "requested user", users: []string{"alex", "player2"}, requested: "player2", want: "player2"},
		{name: "must choose with several users", users: []string{"player2", "alex"}, wantErr: "user required, one of: alex, player2"},
		{name: "unknown user", users: []string{"alex"}, requested: "nobody", wantErr: `user "nobody" not found`},
		{name: "user from another namespace", users: []string{"alex"}, requested: "elsewhere", wantErr: `user "elsewhere" not found`},
		{name: "no users", wantErr: "no users exist to pair with"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newPinTestServer(t, tt.users...)
			got, err := s.resolvePinUser(tt.requested)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("got error %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("got user %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPinUsersHandler(t *testing.T) {
	s := newPinTestServer(t, "player2", "alex")

	rec := httptest.NewRecorder()
	s.pinUsersHandler(rec, httptest.NewRequest("GET", "/pin/users", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("got status %d: %s", rec.Code, rec.Body.String())
	}
	var body struct {
		Users []string `json:"users"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff([]string{"alex", "player2"}, body.Users); diff != "" {
		t.Errorf("users mismatch (-want +got):\n%s", diff)
	}
}

func TestPostPinCarriesUser(t *testing.T) {
	tests := []struct {
		name     string
		users    []string
		body     string
		wantCode int
		wantUser string
	}{
		{name: "chosen user", users: []string{"alex", "player2"}, body: `{"pin":"1234","secret":"s","user":"player2"}`, wantCode: 200, wantUser: "player2"},
		{name: "old page without user, single user", users: []string{"alex"}, body: `{"pin":"1234","secret":"s"}`, wantCode: 200, wantUser: "alex"},
		{name: "old page without user, several users", users: []string{"alex", "player2"}, body: `{"pin":"1234","secret":"s"}`, wantCode: 400},
		{name: "unknown user", users: []string{"alex"}, body: `{"pin":"1234","secret":"s","user":"nobody"}`, wantCode: 400},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := newPinTestServer(t, tt.users...)
			pinChannel := addPendingPin(s, "s", "roth", time.Now())

			rec := httptest.NewRecorder()
			s.pinHandler(rec, httptest.NewRequest("POST", "/pin/", strings.NewReader(tt.body)))

			if rec.Code != tt.wantCode {
				t.Fatalf("got status %d (%s), want %d", rec.Code, rec.Body.String(), tt.wantCode)
			}
			if tt.wantCode != 200 {
				select {
				case got := <-pinChannel:
					t.Fatalf("pairing was unblocked with %+v after a rejected PIN", got)
				default:
				}
				return
			}

			select {
			case got := <-pinChannel:
				if diff := cmp.Diff(pinSubmission{Pin: "1234", Username: tt.wantUser}, got); diff != "" {
					t.Errorf("submission mismatch (-want +got):\n%s", diff)
				}
			case <-time.After(time.Second):
				t.Fatal("no PIN reached the pending pairing")
			}
		})
	}
}
