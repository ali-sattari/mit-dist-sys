package raft

import (
	"log/slog"
	"os"
)

var Logger *slog.Logger

func SetLogLevel(level slog.Level) {
	Logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
	}))
}
