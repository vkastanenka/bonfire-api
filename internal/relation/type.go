package relation

import (
	"bonfire-api/internal/pkg/parse"
)

type Type int

const (
	TypeUnknown Type = iota
	TypePending
	TypeFriends
	TypeBlocked
	typeMax
)

var typeNames = [...]string{
	TypeUnknown: "UNKNOWN",
	TypePending: "PENDING",
	TypeFriends: "FRIENDS",
	TypeBlocked: "BLOCKED",
}

func ParseTypeInt(raw int) (Type, error) {
	return parse.EnumInt(raw, typeMax, "relationType")
}

func ParseTypeString(s string) (Type, error) {
	return parse.EnumString[Type](s, typeNames[:], "relationType")
}

func (t Type) IsValid() bool {
	return t > TypeUnknown && t < typeMax
}

func (p Type) Int() int {
	if p.IsValid() {
		return int(p)
	}
	return int(TypeUnknown)
}

func (t Type) String() string {
	if uint(t) < uint(len(typeNames)) {
		return typeNames[t]
	}
	return typeNames[TypeUnknown]
}

func (t Type) IsPending() bool { return t == TypePending }
func (t Type) IsFriends() bool { return t == TypeFriends }
func (t Type) IsBlocked() bool { return t == TypeBlocked }

func (t Type) MarshalText() ([]byte, error) {
	return []byte(t.String()), nil
}

func (t *Type) UnmarshalText(text []byte) error {
	parsed, err := ParseTypeString(string(text))
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}
