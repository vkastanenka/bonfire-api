package relation

import (
	"net/http"

	"bonfire-api/internal/appctx"
	"bonfire-api/internal/gateway"
	"bonfire-api/internal/httpio"
	"bonfire-api/internal/user"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

type Handler struct {
	service *Service
	bind    *httpio.Bind
}

func NewHandler(service *Service, bind *httpio.Bind) *Handler {
	return &Handler{
		service: service,
		bind:    bind,
	}
}

func (h *Handler) RegisterRoutes(r chi.Router) {
	r.Post("/relations/requests/{id}", httpio.ToHTTPErr(h.sendFriendRequest))
	r.Post("/relations/requests/{id}/accept", httpio.ToHTTPErr(h.acceptFriendRequest))
	r.Post("/relations/blocks/{id}", httpio.ToHTTPErr(h.blockUser))
	r.Delete("/relations/{id}", httpio.ToHTTPErr(h.deleteRelation))
}

type peerIDPath struct {
	ID uuid.UUID `path:"id" validate:"required"`
}

func (h *Handler) sendFriendRequest(w http.ResponseWriter, r *http.Request) error {
	userID, peerID, err := h.bindPeerRequest(r)
	if err != nil {
		return err
	}

	if err := h.service.TransitionPending(r.Context(), userID, peerID); err != nil {
		return err
	}

	httpio.RespondNoContent(w)
	return nil
}

type acceptFriendRequestResponse struct {
	// Channel
	// UserMembership
	// PeerMembership
	PeerUser     *user.DTO        `json:"peerUser,omitempty"`
	PeerPresence gateway.Presence `json:"peerPresence"`
}

func (h *Handler) acceptFriendRequest(w http.ResponseWriter, r *http.Request) error {
	userID, peerID, err := h.bindPeerRequest(r)
	if err != nil {
		return err
	}

	result, err := h.service.TransitionFriends(r.Context(), userID, peerID)
	if err != nil {
		return err
	}

	var peerDTO *user.DTO
	var peerPresence gateway.Presence
	if result != nil {
		peerPresence = result.PeerPresence
		if result.PeerUser != nil {
			peerDTO = user.NewDTO(result.PeerUser)
		}
	}

	httpio.RespondOK(w, r, &acceptFriendRequestResponse{
		PeerUser:     peerDTO,
		PeerPresence: peerPresence,
	})
	return nil
}

func (h *Handler) blockUser(w http.ResponseWriter, r *http.Request) error {
	userID, peerID, err := h.bindPeerRequest(r)
	if err != nil {
		return err
	}

	if err := h.service.TransitionBlocked(r.Context(), userID, peerID); err != nil {
		return err
	}

	httpio.RespondNoContent(w)
	return nil
}

func (h *Handler) deleteRelation(w http.ResponseWriter, r *http.Request) error {
	userID, peerID, err := h.bindPeerRequest(r)
	if err != nil {
		return err
	}

	if err := h.service.DeleteByUserID(r.Context(), userID, peerID); err != nil {
		return err
	}

	httpio.RespondNoContent(w)
	return nil
}

func (h *Handler) bindPeerRequest(r *http.Request) (actorID, peerID uuid.UUID, err error) {
	claims, err := appctx.GetClaims(r.Context())
	if err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	var path peerIDPath
	if err := h.bind.Path(r, &path); err != nil {
		return uuid.Nil, uuid.Nil, err
	}

	return claims.UserID, path.ID, nil
}
