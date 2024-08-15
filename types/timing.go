package types

import (
	"strings"
	"time"
)

type Timing struct {
	Operation string
	Start     time.Time
	End       time.Time
}

func (t *Timing) Duration() time.Duration {
	return t.End.Sub(t.Start)
}

// TryStart checks if start time has not been set and if so, set now
// (and set the operation label)
func (t *Timing) TryStart(operation string) {
	// check if start time is unset
	if t.Start.IsZero() {
		t.Start = time.Now()
		t.Operation = operation
	}
}

type TimingCollection []Timing

func (m TimingCollection) String() string {
	var sb strings.Builder
	sb.WriteString("Timing (may overlap):\n")
	// get max label length
	maxLabelLen := 0
	for _, k := range m {
		if len(k.Operation) > maxLabelLen {
			maxLabelLen = len(k.Operation)
		}
	}

	for _, v := range m {
		sb.WriteString(" - ")
		sb.WriteString(v.Operation)
		sb.WriteString(": ")
		// pad label to max length
		for i := len(v.Operation); i < maxLabelLen; i++ {
			sb.WriteString(" ")
		}
		sb.WriteString(v.Duration().String())
		sb.WriteString("\n")
	}
	return sb.String()
}
