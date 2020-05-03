package sql

import (
	"github.com/stretchr/testify/require"
	"testing"
)

func TestParseTagSetting(t *testing.T) {
	tests := []struct {
		name     string
		tag      string
		expected map[string]string
	}{
		{
			name: "offset parsed correctly",
			tag:  "column:offset",
			expected: map[string]string{
				"COLUMN": "offset",
			},
		},
		{
			name: "created_at parsed correctly",
			tag:  "column:created_at",
			expected: map[string]string{
				"COLUMN": "created_at",
			},
		},
		{
			name: "payload parsed correctly",
			tag:  "column:payload;fromJson",
			expected: map[string]string{
				"COLUMN":   "payload",
				"FROMJSON": "FROMJSON",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := parseTagSetting(tt.tag)

			require.Equal(t, tt.expected, result)
		})
	}
}
