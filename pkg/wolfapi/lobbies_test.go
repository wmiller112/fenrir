package wolfapi_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"games-on-whales.github.io/direwolf/pkg/wolfapi"
)

// wolfServer answers each path with the given JSON and records request bodies
func wolfServer(t *testing.T, replies map[string]string) (wolfapi.Client, map[string]map[string]any) {
	t.Helper()
	bodies := map[string]map[string]any{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reply, ok := replies[r.URL.Path]
		if !ok {
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		if r.Method == http.MethodPost {
			raw, _ := io.ReadAll(r.Body)
			var body map[string]any
			if err := json.Unmarshal(raw, &body); err != nil {
				t.Errorf("%s body is not JSON: %s", r.URL.Path, raw)
			}
			bodies[r.URL.Path] = body
		}
		// Wolf replies with errors as status 500 and a JSON body
		var parsed struct{ Success bool }
		_ = json.Unmarshal([]byte(reply), &parsed)
		if !parsed.Success {
			w.WriteHeader(http.StatusInternalServerError)
		}
		_, _ = io.WriteString(w, reply)
	}))
	t.Cleanup(srv.Close)
	return wolfapi.NewClient(srv.URL, srv.Client()), bodies
}

func TestListLobbies(t *testing.T) {
	client, _ := wolfServer(t, map[string]string{
		"/api/v1/lobbies": `{"success":true,"lobbies":[{"id":"abc","name":"direwolf-ps2","multi_user":true,
			"started_by_profile_id":"direwolf","pin_required":false,"stop_when_everyone_leaves":true,
			"runner":{"type":"process","run_cmd":"sleep"},"connected_sessions":["111","222"]}]}`,
	})

	lobbies, err := client.ListLobbies(context.Background())
	if err != nil {
		t.Fatalf("ListLobbies: %v", err)
	}
	if len(lobbies) != 1 || lobbies[0].ID != "abc" || lobbies[0].Name != "direwolf-ps2" ||
		len(lobbies[0].ConnectedSessions) != 2 || lobbies[0].ConnectedSessions[1] != "222" {
		t.Errorf("unexpected lobbies %+v", lobbies)
	}
}

func TestCreateAndJoinLobby(t *testing.T) {
	client, bodies := wolfServer(t, map[string]string{
		"/api/v1/lobbies/create": `{"success":true,"lobby_id":"abc"}`,
		"/api/v1/lobbies/join":   `{"success":true}`,
	})
	ctx := context.Background()

	id, err := client.CreateLobby(ctx, wolfapi.CreateLobbyRequest{
		ProfileID: "direwolf",
		Name:      "direwolf-ps2",
		MultiUser: true,
		VideoSettings: wolfapi.LobbyVideoSettings{
			Width: 1920, Height: 1080, RefreshRate: 60,
			VideoProducerBufferCaps: "video/x-raw",
		},
		AudioSettings: wolfapi.LobbyAudioSettings{ChannelCount: 2},
		Runner:        wolfapi.Runner{Type: "process", RunCmd: "sleep infinity"},
	})
	if err != nil || id != "abc" {
		t.Fatalf("CreateLobby = %q, %v", id, err)
	}
	created := bodies["/api/v1/lobbies/create"]
	if created["name"] != "direwolf-ps2" || created["multi_user"] != true {
		t.Errorf("unexpected create body %v", created)
	}
	if runner, _ := created["runner"].(map[string]any); runner["type"] != "process" || runner["run_cmd"] != "sleep infinity" {
		t.Errorf("unexpected runner %v", created["runner"])
	}
	if video, _ := created["video_settings"].(map[string]any); video["width"] != 1920.0 || video["video_producer_buffer_caps"] != "video/x-raw" {
		t.Errorf("unexpected video settings %v", created["video_settings"])
	}

	if err := client.JoinLobby(ctx, "abc", "4815162342"); err != nil {
		t.Fatalf("JoinLobby: %v", err)
	}
	joined := bodies["/api/v1/lobbies/join"]
	// wolf:stable takes the session ID as a string here
	if joined["lobby_id"] != "abc" || joined["moonlight_session_id"] != "4815162342" {
		t.Errorf("unexpected join body %v", joined)
	}

	if err := client.JoinLobby(ctx, "abc", "not-a-number"); err == nil {
		t.Error("JoinLobby with a non-numeric session ID succeeded")
	}
}

func TestLobbyErrors(t *testing.T) {
	client, _ := wolfServer(t, map[string]string{
		"/api/v1/lobbies":        `{"success":false,"error":"boom"}`,
		"/api/v1/lobbies/create": `{"success":false,"error":"Lobby setup timed out"}`,
		"/api/v1/lobbies/join":   `{"success":false,"error":"Invalid lobby ID"}`,
	})
	ctx := context.Background()

	if _, err := client.ListLobbies(ctx); err == nil {
		t.Error("ListLobbies succeeded on an error reply")
	}
	if _, err := client.CreateLobby(ctx, wolfapi.CreateLobbyRequest{}); err == nil {
		t.Error("CreateLobby succeeded on an error reply")
	}
	if err := client.JoinLobby(ctx, "nope", "1"); err == nil {
		t.Error("JoinLobby succeeded on an error reply")
	}
}

func TestJoinLobbyNumericSessionIDFallback(t *testing.T) {
	var sent []any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		sent = append(sent, body["moonlight_session_id"])
		// Like a Wolf build that only accepts the session ID as a number
		if _, ok := body["moonlight_session_id"].(float64); !ok {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = io.WriteString(w, `{"success":false,"error":"Failed to parse field 'moonlight_session_id': Could not cast to unsigned long."}`)
			return
		}
		_, _ = io.WriteString(w, `{"success":true}`)
	}))
	t.Cleanup(srv.Close)
	client := wolfapi.NewClient(srv.URL, srv.Client())

	if err := client.JoinLobby(context.Background(), "abc", "42"); err != nil {
		t.Fatalf("JoinLobby: %v", err)
	}
	if len(sent) != 2 || sent[0] != "42" || sent[1] != 42.0 {
		t.Errorf("sent session IDs %v, want the string then the number", sent)
	}
}

func TestListPairedClients(t *testing.T) {
	client, _ := wolfServer(t, map[string]string{
		"/api/v1/clients": `{"success":true,"clients":[{"client_id":"101","app_state_folder":"direwolf-player-1",
			"settings":{"run_uid":1000,"run_gid":1000}}]}`,
	})

	clients, err := client.ListPairedClients(context.Background())
	if err != nil {
		t.Fatalf("ListPairedClients: %v", err)
	}
	if len(clients) != 1 || clients[0].ClientID != "101" || clients[0].AppStateFolder != "direwolf-player-1" {
		t.Errorf("unexpected clients %+v", clients)
	}
}
