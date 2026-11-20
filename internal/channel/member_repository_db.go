package channel

import (
	"context"
	"encoding/json"
	"time"

	"bonfire-api/internal/db"
	"bonfire-api/internal/pkg/errs"

	"github.com/google/uuid"
)

type MemberRepository struct {
	store *db.Store
}

func NewMemberRepository(store *db.Store) *MemberRepository {
	return &MemberRepository{
		store: store,
	}
}

func (r *MemberRepository) CreateBatch(ctx context.Context, members []*Member) ([]*Member, error) {
	if len(members) == 0 {
		return nil, nil
	}

	jsonBytes, err := json.Marshal(newMemberCreatePayloads(members))
	if err != nil {
		return nil, ErrJSONMarshalFailed().Wrap(err)
	}

	rows, err := r.store.ChannelMemberCreateBatch(ctx, jsonBytes)
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	return membersFromRows(rows), nil
}

func (r *MemberRepository) Get(ctx context.Context, channelID, userID uuid.UUID) (*Member, error) {
	row, err := r.store.ChannelMemberGet(ctx, db.ChannelMemberGetParams{
		ChannelID: db.ToUUID(channelID),
		UserID:    db.ToUUID(userID),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	return memberFromRow(row), nil
}

func (r *MemberRepository) GetBatchByChannelIDs(
	ctx context.Context,
	channelIDs []uuid.UUID,
) (map[uuid.UUID][]*Member, error) {
	if len(channelIDs) == 0 {
		return make(map[uuid.UUID][]*Member), nil
	}

	result := make(map[uuid.UUID][]*Member, len(channelIDs))
	for _, id := range channelIDs {
		result[id] = []*Member{}
	}

	rows, err := r.store.ChannelMemberGetBatchByChannelIDs(ctx, db.ToUUIDs(channelIDs))
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	for _, row := range rows {
		m := memberFromRow(row)
		result[m.ChannelID] = append(result[m.ChannelID], m)
	}

	return result, nil
}

func (r *MemberRepository) GetBatchByChannelID(
	ctx context.Context,
	channelID uuid.UUID,
) ([]*Member, error) {
	memberMap, err := r.GetBatchByChannelIDs(ctx, []uuid.UUID{channelID})
	if err != nil {
		return nil, err
	}

	members, ok := memberMap[channelID]
	if !ok || len(members) == 0 {
		return nil, errs.NotFound("entity not found")
	}

	return members, nil
}

func (r *MemberRepository) ListVisibleByUserID(ctx context.Context, userID uuid.UUID, limit int) ([]*Member, error) {
	rows, err := r.store.ChannelMemberListVisibleByUserID(ctx, db.ChannelMemberListVisibleByUserIDParams{
		UserID:   db.ToUUID(userID),
		LimitVal: int32(limit),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	return membersFromRows(rows), nil
}

func (r *MemberRepository) CountByChannelID(ctx context.Context, channelID uuid.UUID) (int, error) {
	count, err := r.store.ChannelMemberCountByChannelID(ctx, db.ToUUID(channelID))
	if err != nil {
		return 0, db.NewError(err, db.ResourceChannelMember)
	}

	return int(count), nil
}

func (r *MemberRepository) CountByUserID(ctx context.Context, userID uuid.UUID) (int, error) {
	count, err := r.store.ChannelMemberCountByUserID(ctx, db.ToUUID(userID))
	if err != nil {
		return 0, db.NewError(err, db.ResourceChannelMember)
	}

	return int(count), nil
}

func (r *MemberRepository) CountBatchByUserID(
	ctx context.Context,
	userIDs []uuid.UUID,
) (map[uuid.UUID]int, error) {
	if len(userIDs) == 0 {
		return make(map[uuid.UUID]int), nil
	}

	rows, err := r.store.ChannelMemberCountBatchByUserID(ctx, db.ToUUIDs(userIDs))
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	counts := make(map[uuid.UUID]int, len(rows))
	for _, row := range rows {
		counts[db.FromUUID(row.UserID)] = int(row.ChannelCount)
	}

	return counts, nil
}

func (r *MemberRepository) UpdateIsVisible(
	ctx context.Context,
	channelID, userID uuid.UUID,
	isVisible bool,
	updatedAt time.Time,
) (*Member, error) {
	row, err := r.store.ChannelMemberUpdateIsVisible(ctx, db.ChannelMemberUpdateIsVisibleParams{
		ChannelID: db.ToUUID(channelID),
		UserID:    db.ToUUID(userID),
		IsVisible: isVisible,
		UpdatedAt: db.ToTimestamptz(updatedAt),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	return memberFromRow(row), nil
}

func (r *MemberRepository) UpdateLastReadMessage(
	ctx context.Context,
	channelID, userID uuid.UUID,
	lastReadMessageID *uuid.UUID,
	lastReadMessageAt, updatedAt time.Time,
	mentionCount *int,
) (*Member, error) {
	row, err := r.store.ChannelMemberUpdateLastReadMessage(ctx, db.ChannelMemberUpdateLastReadMessageParams{
		ChannelID:         db.ToUUID(channelID),
		UserID:            db.ToUUID(userID),
		LastReadMessageID: db.ToUUIDPtr(lastReadMessageID),
		LastReadMessageAt: db.ToTimestamptz(lastReadMessageAt),
		MentionCount:      db.ToInt4Ptr(mentionCount),
		UpdatedAt:         db.ToTimestamptz(updatedAt),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	return memberFromRow(row), nil
}

func (r *MemberRepository) UpdatePinnedAt(
	ctx context.Context,
	channelID, userID uuid.UUID,
	pinnedAt *time.Time,
	updatedAt time.Time,
) (*Member, error) {
	row, err := r.store.ChannelMemberUpdatePinnedAt(ctx, db.ChannelMemberUpdatePinnedAtParams{
		ChannelID: db.ToUUID(channelID),
		UserID:    db.ToUUID(userID),
		PinnedAt:  db.ToTimestamptzPtr(pinnedAt),
		UpdatedAt: db.ToTimestamptz(updatedAt),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	return memberFromRow(row), nil
}

func (r *MemberRepository) UpdateMutedUntil(
	ctx context.Context,
	channelID, userID uuid.UUID,
	mutedUntil *time.Time,
	updatedAt time.Time,
) (*Member, error) {
	row, err := r.store.ChannelMemberUpdateMutedUntil(ctx, db.ChannelMemberUpdateMutedUntilParams{
		ChannelID:  db.ToUUID(channelID),
		UserID:     db.ToUUID(userID),
		MutedUntil: db.ToTimestamptzPtr(mutedUntil),
		UpdatedAt:  db.ToTimestamptz(updatedAt),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	return memberFromRow(row), nil
}

func (r *MemberRepository) IncrementPeersMentionCountByChannelID(
	ctx context.Context,
	channelID, userID uuid.UUID,
	incrementAmount int,
	updatedAt time.Time,
) ([]*Member, error) {
	rows, err := r.store.ChannelMemberIncrementPeersMentionCountByChannelID(ctx, db.ChannelMemberIncrementPeersMentionCountByChannelIDParams{
		IncrementAmount: int32(incrementAmount),
		ChannelID:       db.ToUUID(channelID),
		UserID:          db.ToUUID(userID),
		UpdatedAt:       db.ToTimestamptz(updatedAt),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceChannelMember)
	}

	return membersFromRows(rows), nil
}

func (r *MemberRepository) Delete(ctx context.Context, channelID, userID uuid.UUID) error {
	err := r.store.ChannelMemberDelete(ctx, db.ChannelMemberDeleteParams{
		ChannelID: db.ToUUID(channelID),
		UserID:    db.ToUUID(userID),
	})
	if err != nil {
		return db.NewError(err, db.ResourceChannelMember)
	}

	return nil
}

func memberFromRow(row db.ChannelMember) *Member {
	return ReconstituteMember(
		db.FromUUID(row.ChannelID),
		db.FromUUID(row.UserID),
		db.FromUUIDPtr(row.LastReadMessageID),
		db.FromTimestamptz(row.LastReadMessageAt),
		db.FromTimestamptzPtr(row.PinnedAt),
		db.FromTimestamptzPtr(row.MutedUntil),
		int(row.MentionCount),
		row.IsVisible,
		db.FromTimestamptz(row.CreatedAt),
		db.FromTimestamptz(row.UpdatedAt),
	)
}

func membersFromRows(rows []db.ChannelMember) []*Member {
	if len(rows) == 0 {
		return nil
	}

	members := make([]*Member, 0, len(rows))
	for _, row := range rows {
		members = append(members, memberFromRow(row))
	}

	return members
}

type memberCreatePayload struct {
	ChannelID         uuid.UUID  `json:"channel_id"`
	UserID            uuid.UUID  `json:"user_id"`
	LastReadMessageID *uuid.UUID `json:"last_read_message_id"`
	LastReadMessageAt time.Time  `json:"last_read_message_at"`
	PinnedAt          *time.Time `json:"pinned_at"`
	MutedUntil        *time.Time `json:"muted_until"`
	MentionCount      int32      `json:"mention_count"`
	IsVisible         bool       `json:"is_visible"`
	CreatedAt         time.Time  `json:"created_at"`
	UpdatedAt         time.Time  `json:"updated_at"`
}

func newMemberCreatePayload(m *Member) *memberCreatePayload {
	if m == nil {
		return nil
	}

	return &memberCreatePayload{
		ChannelID:         m.ChannelID,
		UserID:            m.UserID,
		LastReadMessageID: m.LastReadMessageID,
		LastReadMessageAt: m.LastReadMessageAt,
		PinnedAt:          m.PinnedAt,
		MutedUntil:        m.MutedUntil,
		MentionCount:      int32(m.MentionCount),
		IsVisible:         m.IsVisible,
		CreatedAt:         m.CreatedAt,
		UpdatedAt:         m.UpdatedAt,
	}
}

func newMemberCreatePayloads(members []*Member) []*memberCreatePayload {
	payloads := make([]*memberCreatePayload, 0, len(members))
	for _, m := range members {
		if p := newMemberCreatePayload(m); p != nil {
			payloads = append(payloads, p)
		}
	}
	return payloads
}
