package channel

import (
	"bonfire-api/internal/pkg/errs"
	"bonfire-api/internal/pkg/parse"
	"time"
)

type ChannelType int

const (
	ChannelTypeUnknown ChannelType = iota
	ChannelTypeDirect
	ChannelTypeGroup
	channelTypeMax
)

var channelTypeNames = [...]string{
	ChannelTypeUnknown: "UNKNOWN",
	ChannelTypeDirect:  "DIRECT",
	ChannelTypeGroup:   "GROUP",
}

func ParseChTypeInt(raw int) (ChannelType, error) {
	return parse.EnumInt(raw, channelTypeMax, "channel type")
}

func ParseChTypeString(s string) (ChannelType, error) {
	return parse.EnumString[ChannelType](s, channelTypeNames[:], "channel type")
}

func (ct ChannelType) IsValid() bool {
	return ct > ChannelTypeUnknown && ct < channelTypeMax
}

func (ct ChannelType) Int() int {
	if ct.IsValid() {
		return int(ct)
	}
	return int(ChannelTypeUnknown)
}

func (ct ChannelType) String() string {
	if ct.IsValid() {
		return channelTypeNames[ct]
	}
	return channelTypeNames[ChannelTypeUnknown]
}

func (ct ChannelType) IsDirect() bool { return ct == ChannelTypeDirect }
func (ct ChannelType) IsGroup() bool  { return ct == ChannelTypeGroup }

func (ct ChannelType) MarshalText() ([]byte, error) {
	return []byte(ct.String()), nil
}

func (ct *ChannelType) UnmarshalText(text []byte) error {
	parsed, err := ParseChTypeString(string(text))
	if err != nil {
		return err
	}
	*ct = parsed
	return nil
}

type MuteDuration int

const (
	MuteDurationUnknown MuteDuration = iota
	MuteDuration15Min
	MuteDuration1Hour
	MuteDuration8Hours
	MuteDuration24Hours
	MuteDuration3Days
	MuteDurationForever
	muteDurationMax
)

var muteDurationNames = [...]string{
	MuteDurationUnknown: "UNKNOWN",
	MuteDuration15Min:   "15_MIN",
	MuteDuration1Hour:   "1_HOUR",
	MuteDuration8Hours:  "8_HOURS",
	MuteDuration24Hours: "24_HOURS",
	MuteDuration3Days:   "3_DAYS",
	MuteDurationForever: "FOREVER",
}

func ParseMuteDurationInt(raw int) (MuteDuration, error) {
	return parse.EnumInt(raw, muteDurationMax, "mute_duration")
}

func ParseMuteDurationString(s string) (MuteDuration, error) {
	return parse.EnumString[MuteDuration](s, muteDurationNames[:], "mute_duration")
}

func (m MuteDuration) IsValid() bool {
	return m > MuteDurationUnknown && m < muteDurationMax
}

func (m MuteDuration) Int() int {
	if m.IsValid() {
		return int(m)
	}
	return int(MuteDurationUnknown)
}

func (m MuteDuration) String() string {
	if uint(m) < uint(len(muteDurationNames)) {
		return muteDurationNames[m]
	}
	return muteDurationNames[MuteDurationUnknown]
}

func (m MuteDuration) Is15Min() bool   { return m == MuteDuration15Min }
func (m MuteDuration) Is1Hour() bool   { return m == MuteDuration1Hour }
func (m MuteDuration) Is8Hours() bool  { return m == MuteDuration8Hours }
func (m MuteDuration) Is24Hours() bool { return m == MuteDuration24Hours }
func (m MuteDuration) Is3Days() bool   { return m == MuteDuration3Days }
func (m MuteDuration) IsForever() bool { return m == MuteDurationForever }

func (m MuteDuration) ToDuration() (time.Duration, bool) {
	switch m {
	case MuteDuration15Min:
		return 15 * time.Minute, true
	case MuteDuration1Hour:
		return time.Hour, true
	case MuteDuration8Hours:
		return 8 * time.Hour, true
	case MuteDuration24Hours:
		return 24 * time.Hour, true
	case MuteDuration3Days:
		return 72 * time.Hour, true
	case MuteDurationForever:
		return 0, true
	default:
		return 0, false
	}
}

func (m MuteDuration) CalculateUntil(now time.Time) (*time.Time, error) {
	if !m.IsValid() {
		return nil, errs.Internal("Cannot calculate expiry for invalid duration.").
			Meta("enum", "channel_mute_duration")
	}
	if m.IsForever() {
		return nil, nil
	}

	dur, _ := m.ToDuration()
	until := now.Add(dur)
	return &until, nil
}

func (m MuteDuration) MarshalText() ([]byte, error) {
	return []byte(m.String()), nil
}

func (m *MuteDuration) UnmarshalText(text []byte) error {
	parsed, err := ParseMuteDurationString(string(text))
	if err != nil {
		return err
	}
	*m = parsed
	return nil
}
