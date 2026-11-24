package helpers

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSortColumnsAlphabetically(t *testing.T) {
	tests := []struct {
		name string
		in   []string
		want []string
	}{
		{"empty", []string{}, []string{}},
		{"plain names", []string{"c", "a", "b"}, []string{"a", "b", "c"}},
		{"tp_ names last, each group sorted", []string{"tp_timestamp", "zeta", "tp_index", "alpha"}, []string{"alpha", "zeta", "tp_index", "tp_timestamp"}},
		{"only tp_ names", []string{"tp_table", "tp_date"}, []string{"tp_date", "tp_table"}},
		{"a name that only contains tp_ is not a tp_ column", []string{"tp_id", "my_tp_id", "a"}, []string{"a", "my_tp_id", "tp_id"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, SortColumnsAlphabetically(tt.in))
		})
	}
}
