package relation

import (
	"bonfire-api/internal/pkg/errs"
)

func ErrSendingFriendRequest() *errs.Error {
	return errs.Internal("Error sending friend request. Please try again later.")
}

func ErrUnknownRelationFound() *errs.Error {
	return errs.Internal("Error processing relation request, relation with unknown type found in db.")
}

func ErrUserNotFound() *errs.Error {
	return errs.NotFound("Unable to find user with the provided ID.")
}

func ErrUsernameNotFound() *errs.Error {
	return errs.InvalidArgument("Request failed. Please check that the username is correct.").
		FieldViolation("username", "Request failed. Please check that the username is correct.", "USERNAME_INVALID")
}

func ErrUserDisabled() *errs.Error {
	return errs.PermissionDenied("Cannot accept relation that is not pending.").
		Reason("RELATION_NOT_PENDING")
}

func ErrIDGenerateFailed() *errs.Error {
	return errs.Internal("Failed to generate ID.").
		Reason("ID_GENERATE_FAILED")
}

func ErrPeerIDInvalid() *errs.Error {
	return errs.InvalidArgument("Invalid peer ID format.").
		FieldViolation("id", "Invalid ID.", "ID_INVALID")
}

func ErrCannotRequestSelf() *errs.Error {
	return errs.PermissionDenied("Cannot form a relation with yourself.").
		Reason("RELATION_CANNOT_REQUEST_SELF")
}

func ErrMaxOutgoingPendingRequestsReached() *errs.Error {
	return errs.ResourceExhausted("Maximum outgoing friend requests limit reached.").
		Reason("RELATION_MAX_OUTGOING_PENDING_REQUESTS_REACHED")
}

func ErrMaxFriendsReached() *errs.Error {
	return errs.ResourceExhausted("Maximum friends limit reached.").
		Reason("RELATION_MAX_FRIENDS_REACHED")
}

func ErrPeerMaxFriendsReached() *errs.Error {
	return errs.ResourceExhausted("This user has reached the maximum number of friends.").
		Reason("RELATION_PEER_MAX_FRIENDS_REACHED")
}

func ErrPeerMaxIncomingFriendRequests() *errs.Error {
	return errs.ResourceExhausted("This user has reached the maximum number of incoming friend requests.").
		Reason("RELATION_PEER_MAX_INCOMING_FRIEND_REQUESTS_REACHED")
}

func ErrMaxBlocksReached() *errs.Error {
	return errs.ResourceExhausted("Maximum blocked users limit reached.").
		Reason("RELATION_MAX_BLOCKS_REACHED")
}

func ErrNotPending() *errs.Error {
	return errs.PermissionDenied("Cannot accept relation that is not pending.").
		Reason("RELATION_NOT_PENDING")
}

func ErrPendingActor() *errs.Error {
	return errs.PermissionDenied("Cannot accept friend request you sent.").
		Reason("RELATION_PENDING_ACTOR")
}

func ErrAlreadyFriends() *errs.Error {
	return errs.AlreadyExists("Already friends with this user.").
		Reason("RELATION_ALREADY_FRIENDS")
}

func ErrCannotAcceptFriendRequest() *errs.Error {
	return errs.PermissionDenied("Cannot currently accept this friend request.").
		Reason("RELATION_FRIEND_REQUEST_ACCEPTANCE_DENIED")
}

func ErrAlreadyOutgoingBlock() *errs.Error {
	return errs.PermissionDenied("You are already blocking this user.").
		Reason("RELATION_ALREADY_OUTGOING_BLOCK")
}

func ErrAlreadyPending() *errs.Error {
	return errs.AlreadyExists("Friend request already pending.").
		Reason("RELATION_ALREADY_PENDING")
}
