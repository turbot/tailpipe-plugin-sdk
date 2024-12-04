package types

import (
	"strings"
	"sync"
	"time"
)

type Timing struct {
	Operation      string
	Start          time.Time
	End            time.Time
	ActiveDuration time.Duration

	Threads int
	// TODO avoid copying
	mut sync.Mutex
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

func (t *Timing) TotalDuration() time.Duration {
	return t.End.Sub(t.Start)
}

func (t *Timing) UpdateActiveDuration(increment time.Duration) {
	t.mut.Lock()
	defer t.mut.Unlock()
	t.ActiveDuration += increment
}

type TimingCollection []Timing

func (m TimingCollection) String() string {
	var sb strings.Builder
	sb.WriteString("Timing (may overlap):\n")
	// get max label length
	maxLabelLen := 0
	for _, k := range m { //nolint: govet // TODO Timing contains sync.Mutex, find a nice way of handling this
		if len(k.Operation) > maxLabelLen {
			maxLabelLen = len(k.Operation)
		}
	}

	for _, v := range m { //nolint: govet // TODO Timing contains sync.Mutex, find a nice way of handling this
		if v.Operation == "" {
			continue
		}
		sb.WriteString(" - ")
		sb.WriteString(v.Operation)
		sb.WriteString(": ")
		// pad label to max length
		for i := len(v.Operation); i < maxLabelLen; i++ {
			sb.WriteString(" ")
		}
		sb.WriteString(v.TotalDuration().String())
		if v.ActiveDuration > 0 {
			sb.WriteString(" (active: ")
			sb.WriteString(v.ActiveDuration.String())
			sb.WriteString(")")
		}
		sb.WriteString("\n")
	}
	return sb.String()
}
