package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"games-on-whales.github.io/direwolf/pkg/wolfapi"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/klog/v2"
)

// Represents the controller that runs inside the Pod itself
type Agent struct {
	WolfClient wolfapi.Client
}

func NewAgent(
	wolfClient wolfapi.Client,
) *Agent {
	res := &Agent{
		WolfClient: wolfClient,
	}

	return res
}

func (a *Agent) Run(ctx context.Context) error {
	klog.Infof("Starting Agent")
	if err := a.watchEvents(ctx); err != nil {
		return err
	}

	klog.Infof("Agent started")
	return nil
}

func (a *Agent) watchEvents(ctx context.Context) error {
	ch, err := a.WolfClient.SubscribeToEvents(ctx)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev, ok := <-ch:
				if !ok || ev == nil {
					klog.Infof("Event channel closed")
					return
				}

				klog.Infof("Received event: %s", ev.Event)
				klog.Infof("Event ID: %s", ev.ID)
				klog.Infof("Event Data: %s", ev.Data)
				klog.Infof("Event Retry: %d", ev.Retry)
				klog.Infof("Event Comment: %v", ev.Comment)

				switch wolfapi.WolfEventType(ev.Event) {
				// Wolf handles a moonlight disconnect as a "Pause".
				// When moonlight disconnects from Wolf we should reflect that
				// into the state in Kubernetes so things can be cleaned up.
				case wolfapi.PauseStreamEventType:
					var pauseEvent wolfapi.PauseStreamEvent
					if err := json.Unmarshal(ev.Data, &pauseEvent); err != nil {
						utilruntime.HandleError(fmt.Errorf("failed to unmarshal pause stream event: %w", err))
						continue
					}

					if err := a.WolfClient.StopSession(ctx, pauseEvent.SessionID); err != nil {
						utilruntime.HandleError(fmt.Errorf("failed to stop session: %w", err))
						continue
					}
				case wolfapi.VideoSessionEventType:
					var videoSession wolfapi.VideoSessionEvent
					if err := json.Unmarshal(ev.Data, &videoSession); err != nil {
						utilruntime.HandleError(fmt.Errorf("failed to unmarshal video session event: %w", err))
						continue
					}

					go a.joinCoopLobby(ctx, videoSession.SessionID)
				default:
					continue
				}
			}
		}
	}()

	return nil
}

// How long after a stream is set up to join it to the co-op lobby. Its
// pipeline starts on the client's first RTP ping, which comes right after.
var coopJoinDelay = 2 * time.Second

// joinCoopLobby moves a session's stream into the pod's co-op lobby, if the
// operator started one. Wolf only switches streams whose pipeline is already
// running, so this waits for the stream instead of joining when the session
// is created.
func (a *Agent) joinCoopLobby(ctx context.Context, sessionID string) {
	const attempts = 5
	for attempt := 1; attempt <= attempts; attempt++ {
		select {
		case <-ctx.Done():
			return
		case <-time.After(coopJoinDelay):
		}

		joined, err := a.tryJoinCoopLobby(ctx, sessionID)
		if err == nil {
			if joined != "" {
				klog.Infof("Joined session %s to co-op lobby %s", sessionID, joined)
			}
			return
		}
		klog.Errorf("Failed to join session %s to co-op lobby (attempt %d/%d): %v", sessionID, attempt, attempts, err)
	}
}

// tryJoinCoopLobby returns the lobby it joined the session to, or "" if there
// is no co-op lobby or the session is in it already.
func (a *Agent) tryJoinCoopLobby(ctx context.Context, sessionID string) (string, error) {
	lobbies, err := a.WolfClient.ListLobbies(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to list lobbies: %w", err)
	}

	for _, lobby := range lobbies {
		if !strings.HasPrefix(lobby.Name, coopLobbyPrefix) {
			continue
		}
		if slices.Contains(lobby.ConnectedSessions, sessionID) {
			return "", nil
		}
		if err := a.WolfClient.JoinLobby(ctx, lobby.ID, sessionID); err != nil {
			return "", fmt.Errorf("failed to join lobby %s: %w", lobby.ID, err)
		}
		return lobby.ID, nil
	}
	return "", nil
}
