package gateway

import (
	"context"
	"log/slog"

	"github.com/google/uuid"
)

const (
	EventUserPresenceUpdated = "user.presence_updated"
)

func RegisterEvents(h *Hub, b *Broadcaster) {
	h.RegisterHandler(EventUserPresenceUpdated, newEventUserPresenceUpdatedHandler(b))
}

type EventUserPresenceUpdatedPayload struct {
	UserID   string `json:"userId"`
	Presence string `json:"presence"`
}

func newEventUserPresenceUpdatedHandler(b *Broadcaster) MessageHandler {
	return BindMessageHandler(func(ctx context.Context, c ClientContext, p EventUserPresenceUpdatedPayload) error {
		slog.Info("fired newEventUserPresenceUpdatedHandler", "session_id", c.SessionID, "user_id", c.UserID)

		_, err := uuid.Parse(p.UserID)
		if err != nil {
			return nil
		}

		_, err = ParsePresenceString(p.Presence)
		if err != nil {
			return nil
		}

		return b.ToUser(ctx, c.UserID, EventUserPresenceUpdated, p)
	})
}
