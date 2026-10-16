package wolfapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// Lobby is one running app that several Wolf sessions stream from and
// control together.
type Lobby struct {
	ID                     string   `json:"id"`
	Name                   string   `json:"name"`
	MultiUser              bool     `json:"multi_user"`
	StartedByProfileID     string   `json:"started_by_profile_id"`
	PinRequired            bool     `json:"pin_required"`
	StopWhenEveryoneLeaves bool     `json:"stop_when_everyone_leaves"`
	ConnectedSessions      []string `json:"connected_sessions"`
}

type LobbyVideoSettings struct {
	Width                   int    `json:"width"`
	Height                  int    `json:"height"`
	RefreshRate             int    `json:"refresh_rate"`
	WaylandRenderNode       string `json:"wayland_render_node"`
	RunnerRenderNode        string `json:"runner_render_node"`
	VideoProducerBufferCaps string `json:"video_producer_buffer_caps"`
}

type LobbyAudioSettings struct {
	ChannelCount int `json:"channel_count"`
}

type CreateLobbyRequest struct {
	ProfileID              string             `json:"profile_id"`
	Name                   string             `json:"name"`
	MultiUser              bool               `json:"multi_user"`
	StopWhenEveryoneLeaves bool               `json:"stop_when_everyone_leaves"`
	VideoSettings          LobbyVideoSettings `json:"video_settings"`
	AudioSettings          LobbyAudioSettings `json:"audio_settings"`
	RunnerStateFolder      string             `json:"runner_state_folder"`
	Runner                 Runner             `json:"runner"`
}

type LobbiesResponse struct {
	Response `json:",inline"`
	Lobbies  []Lobby `json:"lobbies"`
}

type CreateLobbyResponse struct {
	Response `json:",inline"`
	LobbyID  string `json:"lobby_id"`
}

// GET /api/v1/lobbies
func (c *client) ListLobbies(ctx context.Context) ([]Lobby, error) {
	u, err := url.JoinPath(c.apiURL, "/api/v1/lobbies")
	if err != nil {
		return nil, fmt.Errorf("invalid wolf API URL: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("failed to build request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to list lobbies: %w", err)
	}
	defer resp.Body.Close()

	var lobbiesResp LobbiesResponse
	if err := json.NewDecoder(resp.Body).Decode(&lobbiesResp); err != nil {
		return nil, fmt.Errorf("failed to decode lobbies: %w", err)
	}

	if !lobbiesResp.Success {
		return nil, fmt.Errorf("failed to list lobbies: %s", lobbiesResp.Error)
	}

	return lobbiesResp.Lobbies, nil
}

// POST /api/v1/lobbies/create
//
// Wolf answers once the lobby's compositor is up and its runner started.
func (c *client) CreateLobby(ctx context.Context, lobby CreateLobbyRequest) (string, error) {
	var createResp CreateLobbyResponse
	if err := c.post(ctx, "/api/v1/lobbies/create", lobby, &createResp); err != nil {
		return "", err
	}

	if !createResp.Success {
		return "", fmt.Errorf("failed to create lobby: %s", createResp.Error)
	}

	return createResp.LobbyID, nil
}

// POST /api/v1/lobbies/join
//
// Switches the session's stream and input over to the lobby.
func (c *client) JoinLobby(ctx context.Context, lobbyID, sessionID string) error {
	type JoinLobbyRequest struct {
		LobbyID            string `json:"lobby_id"`
		MoonlightSessionID any    `json:"moonlight_session_id"`
	}

	id, err := strconv.ParseUint(sessionID, 10, 64)
	if err != nil {
		return fmt.Errorf("invalid wolf session ID %q: %w", sessionID, err)
	}

	// Wolf builds disagree on the session ID's type here: wolf:stable wants
	// a string, while some source versions take a number. Try the string
	// first and fall back if Wolf can't parse it.
	var joinResp Response
	for _, sessionIDValue := range []any{sessionID, id} {
		joinResp = Response{}
		if err := c.post(ctx, "/api/v1/lobbies/join", JoinLobbyRequest{
			LobbyID:            lobbyID,
			MoonlightSessionID: sessionIDValue,
		}, &joinResp); err != nil {
			return err
		}
		if joinResp.Success || !strings.Contains(joinResp.Error, "moonlight_session_id") {
			break
		}
	}

	if !joinResp.Success {
		return fmt.Errorf("failed to join lobby: %s", joinResp.Error)
	}

	return nil
}

// post sends body as JSON and decodes the reply into out. Wolf replies with a
// JSON body on errors too, so the status code is not checked.
func (c *client) post(ctx context.Context, path string, body any, out any) error {
	u, err := url.JoinPath(c.apiURL, path)
	if err != nil {
		return fmt.Errorf("invalid wolf API URL: %w", err)
	}

	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("failed to encode request for %s: %w", path, err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(encoded))
	if err != nil {
		return fmt.Errorf("failed to build request for %s: %w", path, err)
	}

	// Same as AddSession: Wolf doesn't accept chunked request bodies
	req.Proto = "HTTP/1.0"
	req.ProtoMajor = 1
	req.ProtoMinor = 0
	req.TransferEncoding = []string{"identity"}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("failed to call %s: %w", path, err)
	}
	defer resp.Body.Close()

	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("failed to decode reply from %s: %w", path, err)
	}
	return nil
}
