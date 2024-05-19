package logger

import (
	"log/slog"
	"os"
)

func New(debug bool) *slog.Logger {
	var logger *slog.Logger

	switch debug {
	case true:
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelDebug,
		}))
	case false:
		logger = slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}))
	}

	return logger
}
