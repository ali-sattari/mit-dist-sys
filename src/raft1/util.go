package raft

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"strconv"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func truncateWithEllipsis(input any, max int) string {
	s := fmt.Sprintf("%v", input)
	runes := []rune(s)
	if len(runes) <= max {
		return s
	}
	if max > 3 {
		return string(runes[:max-3]) + "..."
	}

	return string(runes[:max])
}

func maxIndex(entries []LogEntry) int {
	var m int
	for _, e := range entries {
		m = max(m, e.Id)
	}
	return m
}

func getGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}
