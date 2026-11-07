package gateway

import "bonfire-api/internal/pkg/errs"

func ErrTicketInvalid() *errs.Error {
	return errs.InvalidArgument("Invalid ticket format.").
		FieldViolation("ticket", "Invalid ticket.", "INVALID_TICKET")
}
