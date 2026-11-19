package relation

import (
	"bonfire-api/internal/db"
	"context"

	"github.com/google/uuid"
)

type Repository struct {
	store *db.Store
}

func NewRepository(store *db.Store) *Repository {
	return &Repository{store: store}
}

func (r *Repository) Save(ctx context.Context, rel *Relation) (*Relation, error) {
	if rel == nil {
		return nil, nil
	}

	row, err := r.store.RelationSave(ctx, db.RelationSaveParams{
		User1ID:   db.ToUUID(rel.User1ID),
		User2ID:   db.ToUUID(rel.User2ID),
		ActorID:   db.ToUUID(rel.ActorID),
		ChannelID: db.ToUUIDPtr(rel.ChannelID),
		Type:      int16(rel.Type),
		CreatedAt: db.ToTimestamptz(rel.CreatedAt),
		UpdatedAt: db.ToTimestamptz(rel.UpdatedAt),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceRelation)
	}

	return relationFromRow(row), nil
}

func (r *Repository) Get(ctx context.Context, user1ID, user2ID uuid.UUID) (*Relation, error) {
	row, err := r.store.RelationGet(ctx, db.RelationGetParams{
		User1ID: db.ToUUID(user1ID),
		User2ID: db.ToUUID(user2ID),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceRelation)
	}

	return relationFromRow(row), nil
}

func (r *Repository) GetForUpdate(ctx context.Context, user1ID, user2ID uuid.UUID) (*Relation, error) {
	row, err := r.store.RelationGetForUpdate(ctx, db.RelationGetForUpdateParams{
		User1ID: db.ToUUID(user1ID),
		User2ID: db.ToUUID(user2ID),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceRelation)
	}

	return relationFromRow(row), nil
}

func (r *Repository) GetByChannelID(ctx context.Context, channelID uuid.UUID) (*Relation, error) {
	row, err := r.store.RelationGetByChannelID(ctx, db.ToUUID(channelID))
	if err != nil {
		return nil, db.NewError(err, db.ResourceRelation)
	}

	return relationFromRow(row), nil
}

func (r *Repository) ListTypeByUserID(
	ctx context.Context,
	userID uuid.UUID,
	relType Type,
	limit int,
) ([]*Relation, error) {
	rows, err := r.store.RelationListTypeByUserID(ctx, db.RelationListTypeByUserIDParams{
		UserID:   db.ToUUID(userID),
		Type:     int16(relType),
		LimitVal: int32(limit),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceRelation)
	}

	return relationsFromRows(rows), nil
}

func (r *Repository) ListFriendsByUserID(
	ctx context.Context,
	userID uuid.UUID,
	limit int,
) ([]*Relation, error) {
	return r.ListTypeByUserID(ctx, userID, TypeFriends, limit)
}

func (r *Repository) ListIncomingTypeByUserID(
	ctx context.Context,
	userID uuid.UUID,
	relType Type,
	limit int,
) ([]*Relation, error) {
	rows, err := r.store.RelationListIncomingTypeByUserID(ctx, db.RelationListIncomingTypeByUserIDParams{
		UserID:   db.ToUUID(userID),
		Type:     int16(relType),
		LimitVal: int32(limit),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceRelation)
	}

	return relationsFromRows(rows), nil
}

func (r *Repository) ListOutgoingTypeByUserID(
	ctx context.Context,
	userID uuid.UUID,
	relType Type,
	limit int,
) ([]*Relation, error) {
	rows, err := r.store.RelationListOutgoingTypeByUserID(ctx, db.RelationListOutgoingTypeByUserIDParams{
		UserID:   db.ToUUID(userID),
		Type:     int16(relType),
		LimitVal: int32(limit),
	})
	if err != nil {
		return nil, db.NewError(err, db.ResourceRelation)
	}

	return relationsFromRows(rows), nil
}

func (r *Repository) ListIncomingPendingsByUserID(
	ctx context.Context,
	userID uuid.UUID,
	limit int,
) ([]*Relation, error) {
	return r.ListIncomingTypeByUserID(ctx, userID, TypePending, limit)
}

func (r *Repository) ListOutgoingPendingsByUserID(
	ctx context.Context,
	userID uuid.UUID,
	limit int,
) ([]*Relation, error) {
	return r.ListOutgoingTypeByUserID(ctx, userID, TypePending, limit)
}

func (r *Repository) ListIncomingBlocksByUserID(
	ctx context.Context,
	userID uuid.UUID,
	limit int,
) ([]*Relation, error) {
	return r.ListIncomingTypeByUserID(ctx, userID, TypeBlocked, limit)
}

func (r *Repository) ListOutgoingBlocksByUserID(
	ctx context.Context,
	userID uuid.UUID,
	limit int,
) ([]*Relation, error) {
	return r.ListOutgoingTypeByUserID(ctx, userID, TypeBlocked, limit)
}

func (r *Repository) CountTypeByUserID(
	ctx context.Context,
	userID uuid.UUID,
	relType Type,
) (int, error) {
	count, err := r.store.RelationCountTypeByUserID(ctx, db.RelationCountTypeByUserIDParams{
		UserID: db.ToUUID(userID),
		Type:   int16(relType),
	})
	if err != nil {
		return 0, db.NewError(err, db.ResourceRelation)
	}

	return int(count), nil
}

func (r *Repository) CountFriendsByUserID(
	ctx context.Context,
	userID uuid.UUID,
) (int, error) {
	return r.CountTypeByUserID(ctx, userID, TypeFriends)
}

func (r *Repository) CountIncomingTypeByUserID(
	ctx context.Context,
	userID uuid.UUID,
	relType Type,
) (int, error) {
	count, err := r.store.RelationCountIncomingTypeByUserID(ctx, db.RelationCountIncomingTypeByUserIDParams{
		UserID: db.ToUUID(userID),
		Type:   int16(relType),
	})
	if err != nil {
		return 0, db.NewError(err, db.ResourceRelation)
	}

	return int(count), nil
}

func (r *Repository) CountOutgoingTypeByUserID(
	ctx context.Context,
	userID uuid.UUID,
	relType Type,
) (int, error) {
	count, err := r.store.RelationCountOutgoingTypeByUserID(ctx, db.RelationCountOutgoingTypeByUserIDParams{
		UserID: db.ToUUID(userID),
		Type:   int16(relType),
	})
	if err != nil {
		return 0, db.NewError(err, db.ResourceRelation)
	}

	return int(count), nil
}

func (r *Repository) CountIncomingPendingsByUserID(
	ctx context.Context,
	userID uuid.UUID,
) (int, error) {
	return r.CountIncomingTypeByUserID(ctx, userID, TypePending)
}

func (r *Repository) CountOutgoingPendingsByUserID(
	ctx context.Context,
	userID uuid.UUID,
) (int, error) {
	return r.CountOutgoingTypeByUserID(ctx, userID, TypePending)
}

func (r *Repository) CountIncomingBlocksByUserID(
	ctx context.Context,
	userID uuid.UUID,
) (int, error) {
	return r.CountIncomingTypeByUserID(ctx, userID, TypeBlocked)
}

func (r *Repository) CountOutgoingBlocksByUserID(
	ctx context.Context,
	userID uuid.UUID,
) (int, error) {
	return r.CountOutgoingTypeByUserID(ctx, userID, TypeBlocked)
}

func (r *Repository) HasIncomingBlock(ctx context.Context, actorID uuid.UUID, peerIDs []uuid.UUID) (bool, error) {
	if len(peerIDs) == 0 {
		return false, nil
	}

	hasBlock, err := r.store.RelationHasIncomingBlock(ctx, db.RelationHasIncomingBlockParams{
		ActorID: db.ToUUID(actorID),
		PeerIds: db.ToUUIDs(peerIDs),
	})
	if err != nil {
		return false, db.NewError(err, db.ResourceRelation)
	}

	return hasBlock, nil
}

func (r *Repository) DeleteByUserID(ctx context.Context, user1ID, user2ID, actorID uuid.UUID) error {
	err := r.store.RelationDeleteByUserID(ctx, db.RelationDeleteByUserIDParams{
		User1ID: db.ToUUID(user1ID),
		User2ID: db.ToUUID(user2ID),
		ActorID: db.ToUUID(actorID),
	})
	if err != nil {
		return db.NewError(err, db.ResourceRelation)
	}

	return nil
}

func relationFromRow(row db.Relation) *Relation {
	return Reconstitute(
		db.FromUUID(row.User1ID),
		db.FromUUID(row.User2ID),
		db.FromUUID(row.ActorID),
		db.FromUUIDPtr(row.ChannelID),
		Type(int(row.Type)),
		db.FromTimestamptz(row.CreatedAt),
		db.FromTimestamptz(row.UpdatedAt),
	)
}

func relationsFromRows(rows []db.Relation) []*Relation {
	relations := make([]*Relation, 0, len(rows))
	for _, row := range rows {
		relations = append(relations, relationFromRow(row))
	}
	return relations
}
