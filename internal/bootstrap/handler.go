package bootstrap

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
	r.Get("/users/@me", httpio.ToHTTPErr(h.bootstrap))
}

type bootstrapResponse struct {
	Me                 *user.MeDTO                       `json:"me"`
	OutgoingPendingIDs []uuid.UUID                       `json:"outgoingPendingIds"`
	IncomingPendingIDs []uuid.UUID                       `json:"incomingPendingIds"`
	FriendIDs          []uuid.UUID                       `json:"friendIds"`
	OutgoingBlockIDs   []uuid.UUID                       `json:"outgoingBlockIds"`
	UserMembers        map[uuid.UUID]*channel.MemberDTO  `json:"userChannelMembers"`
	Channels           map[uuid.UUID]*channel.ChannelDTO `json:"channels"`
	ChannelMemberIDs   map[uuid.UUID][]uuid.UUID         `json:"channelMemberIds"`
	SidebarIDs         []uuid.UUID                       `json:"sidebarIds"`
	Peers              map[uuid.UUID]*user.DTO           `json:"peers"`
	PeerPresences      map[uuid.UUID]gateway.Presence    `json:"peerPresences"`
}

func (h *Handler) bootstrap(w http.ResponseWriter, r *http.Request) error {
	claims, err := appctx.GetClaims(r.Context())
	if err != nil {
		return err
	}

	result, err := h.service.Bootstrap(r.Context(), Params{
		UserID: claims.UserID,
	})
	if err != nil {
		return err
	}

	httpio.RespondOK(w, r, bootstrapResponse{
		Me:                 user.NewMeDTO(result.User),
		OutgoingPendingIDs: result.OutgoingPendingIDs,
		IncomingPendingIDs: result.IncomingPendingIDs,
		FriendIDs:          result.FriendIDs,
		OutgoingBlockIDs:   result.OutgoingBlockIDs,
		UserMembers:        channel.NewMemberDTOsMap(result.UserMembers),
		Channels:           channel.NewDTOsMap(result.Channels),
		ChannelMemberIDs:   result.ChannelMemberIDs,
		SidebarIDs:         result.SidebarIDs,
		Peers:              user.NewDTOsMap(result.Peers),
		PeerPresences:      result.PeerPresences,
	})
	return nil
}
