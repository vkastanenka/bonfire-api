package relation

import (
	"net/http"

	"bonfire-api/internal/appctx"
	"bonfire-api/internal/channel"
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
	r.Post("/relations/requests", httpio.ToHTTPErr(h.sendFriendRequest))
	r.Post("/relations/requests/{id}/accept", httpio.ToHTTPErr(h.acceptFriendRequest))
	r.Post("/relations/blocks/{id}", httpio.ToHTTPErr(h.blockUser))
	r.Delete("/relations/{id}", httpio.ToHTTPErr(h.deleteRelation))
}

type sendFriendRequest struct {
	Username string `json:"username" mod:"text" validate:"required,user_username"`
}

func (h *Handler) sendFriendRequest(w http.ResponseWriter, r *http.Request) error {
	claims, err := appctx.GetClaims(r.Context())
	if err != nil {
		return err
	}

	var req sendFriendRequest
	if err = h.bind.JSON(w, r, &req); err != nil {
		return err
	}

	result, err := h.service.TransitionPending(r.Context(), claims.UserID, req.Username)
	if err != nil {
		return err
	}

	resp := newFriendAddedResponse(result)
	if resp == nil {
		return ErrSendingFriendRequest()
	}

	httpio.RespondOK(w, r, resp)
	return nil
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

	httpio.RespondOK(w, r, newFriendAddedResponse(result))
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

type peerIDPath struct {
	ID uuid.UUID `path:"id" validate:"required"`
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

type friendAddedResponse struct {
	Channel      *channel.ChannelDTO `json:"channel"`
	Membership   *channel.MemberDTO  `json:"membership"`
	PeerUser     *user.DTO           `json:"peerUser"`
	PeerPresence gateway.Presence    `json:"peerPresence"`
}

func newFriendAddedResponse(result *TransitionResult) *friendAddedResponse {
	if result == nil || result.PeerUser == nil {
		return nil
	}

	return &friendAddedResponse{
		Channel:      channel.NewDTO(result.Channel),
		Membership:   channel.NewMemberDTO(result.Membership),
		PeerUser:     user.NewDTO(result.PeerUser),
		PeerPresence: *result.PeerPresence,
	}
}
