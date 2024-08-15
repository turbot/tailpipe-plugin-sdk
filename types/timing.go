package types

import (
	"strings"
	"time"
)

type Timing struct {
	Start time.Time
	End   time.Time
}

func (t *Timing) Duration() time.Duration {
	return t.End.Sub(t.Start)
}

type TimingMap map[string]Timing

func (m TimingMap) String() string {
	var sb strings.Builder
	sb.WriteString("Timing:\n")
	// get max label length
	maxLabelLen := 0
	for k := range m {
		if len(k) > maxLabelLen {
			maxLabelLen = len(k)
		}
	}

	for k, v := range m {
		sb.WriteString(k)
		sb.WriteString(":")
		// pad label to max length
		for i := len(k); i < maxLabelLen; i++ {
			sb.WriteString(" ")
		}
		sb.WriteString(v.Duration().String())
		sb.WriteString("\n")
	}
	return sb.String()
}
