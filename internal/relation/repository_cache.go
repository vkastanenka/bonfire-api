package relation

import (
	"context"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type CachedRepository struct {
	cache *Cache
	repo  *Repository
}

func NewCachedRepository(cache *Cache, repo *Repository) *CachedRepository {
	return &CachedRepository{
		cache: cache,
		repo:  repo,
	}
}

func (r *CachedRepository) GetPendingIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return r.getCachedIDs(
		ctx,
		userID,
		"pendings",
		r.cache.GetPendingIDs,
		r.repo.ListPendingsByUserID,
		r.cache.SetPendingIDs,
	)
}

func (r *CachedRepository) GetFriendIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return r.getCachedIDs(
		ctx,
		userID,
		"friends",
		r.cache.GetFriendIDs,
		r.repo.ListFriendsByUserID,
		r.cache.SetFriendIDs,
	)
}

func (r *CachedRepository) GetIncomingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return r.getCachedIDs(
		ctx,
		userID,
		"incoming_blocks",
		r.cache.GetIncomingBlockIDs,
		r.repo.ListIncomingBlocksByUserID,
		r.cache.SetIncomingBlockIDs,
	)
}

func (r *CachedRepository) GetOutgoingBlockIDs(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error) {
	return r.getCachedIDs(
		ctx,
		userID,
		"outgoing_blocks",
		r.cache.GetOutgoingBlockIDs,
		r.repo.ListOutgoingBlocksByUserID,
		r.cache.SetOutgoingBlockIDs,
	)
}

type listRelationsFn func(ctx context.Context, userID uuid.UUID, limit int) ([]*Relation, error)
type getCachedIDsFn func(ctx context.Context, userID uuid.UUID) ([]uuid.UUID, error)
type setCachedIDsFn func(ctx context.Context, userID uuid.UUID, ids []uuid.UUID) error

func (r *CachedRepository) getCachedIDs(
	ctx context.Context,
	userID uuid.UUID,
	relType string,
	getCache getCachedIDsFn,
	listDB listRelationsFn,
	setCache setCachedIDsFn,
) ([]uuid.UUID, error) {
	ids, err := getCache(ctx, userID)
	if err == nil && len(ids) > 0 {
		return ids, nil
	}
	if err != nil {
		slog.WarnContext(ctx, "redis relation ids cache read failure, falling back to database",
			slog.String("type", relType),
			slog.String("user_id", userID.String()),
			slog.Any("error", err),
		)
	}

	rels, err := listDB(ctx, userID, MaxTypeLimit)
	if err != nil {
		return nil, err
	}

	peerIDs := ExtractPeerIDs(userID, rels)
	if len(peerIDs) > 0 {
		cacheCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()

		if setErr := setCache(cacheCtx, userID, peerIDs); setErr != nil {
			slog.WarnContext(cacheCtx, "failed to populate relation ids cache after database read",
				slog.String("type", relType),
				slog.String("user_id", userID.String()),
				slog.Any("error", setErr),
			)
		}
	}

	return peerIDs, nil
}
