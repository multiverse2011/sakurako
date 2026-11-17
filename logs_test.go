package main

import (
	"fmt"
	"testing"
)

func TestLogRingKeepsNewest(t *testing.T) {
	r := &logRing{}
	for i := 0; i < logLimit+5; i++ {
		if _, err := r.Write([]byte(fmt.Sprintf("line %d\n", i))); err != nil {
			t.Fatal(err)
		}
	}
	entries, next := r.since(0)
	if next != logLimit+5 || len(entries) != logLimit {
		t.Fatalf("next=%d len=%d", next, len(entries))
	}
	if entries[0].Text != "line 5" || entries[len(entries)-1].Text != fmt.Sprintf("line %d", logLimit+4) {
		t.Fatalf("unexpected window: %q .. %q", entries[0].Text, entries[len(entries)-1].Text)
	}
	entries, _ = r.since(int64(logLimit + 4))
	if len(entries) != 1 || entries[0].Text != fmt.Sprintf("line %d", logLimit+4) {
		t.Fatalf("since: %+v", entries)
	}
	r.Write([]byte("\n"))
	if entries, _ := r.since(0); len(entries) != logLimit {
		t.Fatalf("empty write stored a line: %d", len(entries))
	}
}
