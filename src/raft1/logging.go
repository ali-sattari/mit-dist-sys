package raft

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"

	tester "6.5840/tester1"
)

func (rf *Raft) setupLogging() {
	fileHandler := slog.NewTextHandler(getLogOutputPath(rf.me), &slog.HandlerOptions{
		Level:     getLogLevel(),
		AddSource: true,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.SourceKey {
				source := a.Value.Any().(*slog.Source)
				source.File = filepath.Base(source.File)
			}
			return a
		},
	})
	annotateHandler := &AnnotateHandler{
		next: fileHandler,
	}

	rf.logger = slog.New(annotateHandler).With(
		"server", rf.me,
		"role", rf.nodeRole,
	)
}

func getLogLevel() slog.Level {
	var level slog.Level
	err := level.UnmarshalText([]byte(os.Getenv("LOG_LEVEL")))
	if err != nil {
		return LOG_LEVEL
	}
	return level
}

func getLogOutputPath(server int) io.Writer {
	logPath := os.Getenv("LOG_FILE_NAME")
	if logPath == "" {
		return os.Stderr
	}

	file, err := os.OpenFile(fmt.Sprintf("%s-%d.log", logPath, server), os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}
	return file
}

type AnnotateHandler struct {
	next   slog.Handler
	server int
	role   NodeRole
	attrs  []slog.Attr
}

func (h *AnnotateHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return true
}

func (h *AnnotateHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	var newH AnnotateHandler

	// Copy existing attrs to avoid mutating the receiver.
	newAttrs := make([]slog.Attr, len(h.attrs), len(h.attrs)+len(attrs))
	copy(newAttrs, h.attrs)
	newAttrs = append(newAttrs, attrs...)
	newH.attrs = newAttrs

	for _, a := range attrs {
		switch a.Key {
		case "server":
			newH.server = int(a.Value.Int64())
		case "role":
			if r, err := ParseNodeRole(a.Value.String()); err == nil {
				newH.role = r
			}
		}
	}

	var newNext slog.Handler
	if h.next != nil {
		newNext = h.next.WithAttrs(attrs)
	}
	newH.next = newNext

	return &newH
}

func (h *AnnotateHandler) WithGroup(name string) slog.Handler {
	return h
}

var RoleColor = map[NodeRole]string{
	Leader:    "#FF8C00",
	Candidate: "#8FBC8F",
	Follower:  "#B0C4DE",
}

func (h *AnnotateHandler) Handle(ctx context.Context, r slog.Record) error {
	tag := fmt.Sprintf("server %d", h.server)
	desp := r.Message
	var details string
	r.Attrs(func(a slog.Attr) bool {
		details += fmt.Sprintf("%s: %+v<br/>", a.Key, a.Value)
		return true
	})

	if r.Level >= slog.LevelDebug {
		tester.AnnotatePointColor(tag, desp, details, RoleColor[h.role])
	}

	if h.next != nil {
		return h.next.Handle(ctx, r)
	}

	return nil
}
