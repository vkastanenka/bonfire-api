package relation

import (
	"bonfire-api/internal/pkg/errs"
)

func ErrTypeInvalid() *errs.Error {
	return errs.InvalidArgument("Invalid relationship type.").
		Reason("RELATIONSHIP_TYPE_INVALID").
		FieldViolation("type", "Must be one of: pending, friends, blocked.", "INVALID_ENUM_VALUE")
}

func ErrPeerIDInvalid() *errs.Error {
	return errs.InvalidArgument("Relation ids cannot match.").
		FieldViolation("peer_id", "ID is the same as user ID", "PEER_ID_INVALID")
}

func ErrCannotRequestSelf() *errs.Error {
	return errs.PermissionDenied("Cannot form a relation with yourself.").
		Reason("RELATION_CANNOT_REQUEST_SELF")
}

func ErrPendingRequestNotFound() *errs.Error {
	return errs.NotFound("Pending friend request not found.").
		Reason("RELATION_PENDING_NOT_FOUND")
}

func ErrMaxPendingRequestsReached() *errs.Error {
	return errs.ResourceExhausted("Maximum outgoing friend requests limit reached.").
		Reason("RELATION_MAX_PENDING_REACHED")
}

func ErrMaxFriendsReached() *errs.Error {
	return errs.ResourceExhausted("Maximum friends limit reached.").
		Reason("RELATION_MAX_FRIENDS_REACHED")
}

func ErrBlockedUser() *errs.Error {
	return errs.PermissionDenied("Action cannot be performed on a blocked relationship.").
		Reason("RELATION_BLOCKED_USER")
}

func ErrNotPending() *errs.Error {
	return errs.PermissionDenied("Cannot accept relation that is not pending.").
		Reason("RELATION_NOT_PENDING")
}

func ErrAlreadyFriends() *errs.Error {
	return errs.AlreadyExists("Already friends with this user.").
		Reason("RELATION_ALREADY_FRIENDS")
}

func ErrPermissionDenied() *errs.Error {
	return errs.PermissionDenied("Cannot interact with this user.").
		Reason("RELATION_PERMISSION_DENIED")
}

func ErrAlreadyPending() *errs.Error {
	return errs.AlreadyExists("Friend request already pending.").
		Reason("RELATION_ALREADY_PENDING")
}
