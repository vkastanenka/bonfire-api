package channel

import "bonfire-api/internal/pkg/parse"

type MessageType int

const (
	MessageTypeUnknown MessageType = iota
	MessageTypeDefault
	MessageTypeReply
	MessageTypeForward
	MessageTypeMemberAdd
	MessageTypeMemberRemove
	MessageTypeNameChange
	MessageTypeIconChange
	MessageTypePin
	messageTypeMax
)

var messageTypeNames = [...]string{
	MessageTypeUnknown:      "UNKNOWN",
	MessageTypeDefault:      "DEFAULT",
	MessageTypeReply:        "REPLY",
	MessageTypeForward:      "FORWARD",
	MessageTypeMemberAdd:    "MEMBER_ADD",
	MessageTypeMemberRemove: "MEMBER_REMOVE",
	MessageTypeNameChange:   "NAME_CHANGE",
	MessageTypeIconChange:   "ICON_CHANGE",
	MessageTypePin:          "PIN",
}

func ParseMsgTypeInt(raw int) (MessageType, error) {
	return parse.EnumInt(raw, messageTypeMax, "message type")
}

func ParseMsgTypeString(s string) (MessageType, error) {
	return parse.EnumString[MessageType](s, messageTypeNames[:], "message type")
}

func (mt MessageType) IsValid() bool {
	return mt > MessageTypeUnknown && mt < messageTypeMax
}

func (mt MessageType) Int() int {
	if mt.IsValid() {
		return int(mt)
	}
	return int(MessageTypeUnknown)
}

func (mt MessageType) String() string {
	if mt.IsValid() {
		return messageTypeNames[mt]
	}
	return messageTypeNames[MessageTypeUnknown]
}

func (mt MessageType) IsDefault() bool      { return mt == MessageTypeDefault }
func (mt MessageType) IsReply() bool        { return mt == MessageTypeReply }
func (mt MessageType) IsForward() bool      { return mt == MessageTypeForward }
func (mt MessageType) IsMemberAdd() bool    { return mt == MessageTypeMemberAdd }
func (mt MessageType) IsMemberRemove() bool { return mt == MessageTypeMemberRemove }
func (mt MessageType) IsNameChange() bool   { return mt == MessageTypeNameChange }
func (mt MessageType) IsIconChange() bool   { return mt == MessageTypeIconChange }
func (mt MessageType) IsPin() bool          { return mt == MessageTypePin }

func (mt MessageType) MarshalText() ([]byte, error) {
	return []byte(mt.String()), nil
}

func (mt *MessageType) UnmarshalText(text []byte) error {
	parsed, err := ParseMsgTypeString(string(text))
	if err != nil {
		return err
	}
	*mt = parsed
	return nil
}
