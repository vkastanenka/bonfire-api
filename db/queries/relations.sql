-- name: RelationSave :one
INSERT INTO relations(user1_id, user2_id, actor_id, channel_id, created_at, updated_at, type, is_active)
    VALUES (LEAST(@user1_id::uuid, @user2_id::uuid), GREATEST(@user1_id::uuid, @user2_id::uuid), @actor_id::uuid, sqlc.narg('channel_id')::uuid, @created_at::timestamptz, @updated_at::timestamptz, @type::smallint, @is_active::boolean)
ON CONFLICT (user1_id, user2_id)
    DO UPDATE SET
        actor_id = EXCLUDED.actor_id,
        channel_id = COALESCE(EXCLUDED.channel_id, relations.channel_id),
        updated_at = EXCLUDED.updated_at,
        type = EXCLUDED.type,
        is_active = EXCLUDED.is_active
    RETURNING
        relations.*;

-- name: RelationGet :one
SELECT
    relations.*
FROM
    relations
WHERE
    user1_id = LEAST(@user1_id::uuid, @user2_id::uuid)
    AND user2_id = GREATEST(@user1_id::uuid, @user2_id::uuid);

-- name: RelationGetForUpdate :one
SELECT
    relations.*
FROM
    relations
WHERE
    user1_id = LEAST(@user1_id::uuid, @user2_id::uuid)
    AND user2_id = GREATEST(@user1_id::uuid, @user2_id::uuid)
FOR UPDATE;

-- name: RelationListTypeByUserID :many
SELECT
    relations.*
FROM
    relations
WHERE (user1_id = @user_id::uuid
    OR user2_id = @user_id::uuid)
AND is_active = TRUE
AND type = @type::smallint
ORDER BY
    created_at DESC
LIMIT @limit_val::int;

-- name: RelationListIncomingTypeByUserID :many
SELECT
    relations.*
FROM
    relations
WHERE (user1_id = @user_id::uuid
    OR user2_id = @user_id::uuid)
AND is_active = TRUE
AND type = @type::smallint
AND actor_id != @user_id::uuid
ORDER BY
    created_at DESC
LIMIT @limit_val::int;

-- name: RelationListOutgoingTypeByUserID :many
SELECT
    relations.*
FROM
    relations
WHERE (user1_id = @user_id::uuid
    OR user2_id = @user_id::uuid)
AND is_active = TRUE
AND type = @type::smallint
AND actor_id = @user_id::uuid
ORDER BY
    created_at DESC
LIMIT @limit_val::int;

-- name: RelationListPendingsAndFriendsByUserID :many
SELECT
    relations.*
FROM
    relations
WHERE (user1_id = @user_id::uuid
    OR user2_id = @user_id::uuid)
AND is_active = TRUE
AND type IN (1, 2)
ORDER BY
    created_at DESC
LIMIT @limit_val::int;

-- name: RelationCountTypeByUserID :one
SELECT
    COUNT(*)
FROM
    relations
WHERE (user1_id = @user_id::uuid
    OR user2_id = @user_id::uuid)
AND is_active = TRUE
AND type = @type::smallint;

-- name: RelationCountIncomingTypeByUserID :one
SELECT
    COUNT(*)
FROM
    relations
WHERE (user1_id = @user_id::uuid
    OR user2_id = @user_id::uuid)
AND is_active = TRUE
AND type = @type::smallint
AND actor_id != @user_id::uuid;

-- name: RelationCountOutgoingTypeByUserID :one
SELECT
    COUNT(*)
FROM
    relations
WHERE (user1_id = @user_id::uuid
    OR user2_id = @user_id::uuid)
AND is_active = TRUE
AND type = @type::smallint
AND actor_id = @user_id::uuid;

-- name: RelationHasIncomingBlock :one
SELECT
    EXISTS (
        SELECT
            1
        FROM
            relations
        WHERE ((user1_id = @actor_id::uuid
                AND user2_id = ANY (@peer_ids::uuid[]))
            OR (user2_id = @actor_id::uuid
                AND user1_id = ANY (@peer_ids::uuid[])))
        AND is_active = TRUE
        AND type = 3
        AND actor_id != @actor_id::uuid);

-- name: RelationDeactivateByUserID :exec
UPDATE
    relations
SET
    is_active = FALSE,
    updated_at = @updated_at::timestamptz
WHERE
    user1_id = LEAST(@user1_id::uuid, @user2_id::uuid)
    AND user2_id = GREATEST(@user1_id::uuid, @user2_id::uuid)
    AND is_active = TRUE
    AND (type != 3
        OR actor_id = @actor_id::uuid);

-- name: RelationDeleteByUserID :exec
DELETE FROM relations
WHERE user1_id = LEAST(@user1_id::uuid, @user2_id::uuid)
    AND user2_id = GREATEST(@user1_id::uuid, @user2_id::uuid)
    AND channel_id IS NULL
    AND is_active = TRUE
    AND (type != 3
        OR actor_id = @actor_id::uuid);

