package logging

import (
	"fmt"
	"github.com/turbot/pipe-fittings/constants"
	"github.com/turbot/pipe-fittings/sanitize"
	"io"
	"log/slog"
	"os"
	"strings"
)

func Initialize(pluginName string) {
	slog.SetDefault(tailpipePluginLogger(pluginName))
}

// tailpipePluginLogger returns a logger that writes to stderr and sanitizes log entries
func tailpipePluginLogger(pluginName string) *slog.Logger {
	level := getLogLevel()
	if level == constants.LogLevelOff {
		return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
	}

	handlerOptions := &slog.HandlerOptions{
		Level: level,

		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			sanitized := sanitize.Instance.SanitizeKeyValue(a.Key, a.Value.Any())

			return slog.Attr{
				Key:   a.Key,
				Value: slog.AnyValue(sanitized),
			}
		},
	}
	// add plugin name as source
	pluginLongName := fmt.Sprintf("tailpipe-plugin-%s", pluginName)
	return slog.New(slog.NewJSONHandler(os.Stderr, handlerOptions)).With("source", pluginLongName)
}

func getLogLevel() slog.Leveler {
	levelEnv := os.Getenv(EnvLogLevel)

	switch strings.ToLower(levelEnv) {
	case "debug":
		return slog.LevelDebug
	case "info":
		return slog.LevelInfo
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "off":
		return constants.LogLevelOff
	default:
		return constants.LogLevelOff
	}
}
