package raft

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// FileStorage persists Raft's currentTerm/votedFor/log to a single JSON
// file, written via write-to-temp-then-atomic-rename so a crash mid-write
// never leaves a corrupt, partially-written state file behind (the rename
// either lands the old file or the new one, never a half-written mix).
//
// Simplification: JSON re-serializes the entire log on every save, which is
// O(log length) per write rather than an append-only format. Fine at demo
// scale; a production implementation would append new entries and
// periodically snapshot+truncate instead.
type FileStorage struct {
	path string
}

func NewFileStorage(dir string) (*FileStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &FileStorage{path: filepath.Join(dir, "raft-state.json")}, nil
}

type persistedState struct {
	CurrentTerm int        `json:"currentTerm"`
	VotedFor    string     `json:"votedFor"`
	Log         []LogEntry `json:"log"`
}

func (fs *FileStorage) SaveState(currentTerm int, votedFor string, log []LogEntry) error {
	state := persistedState{CurrentTerm: currentTerm, VotedFor: votedFor, Log: log}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	tmpPath := fs.path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, fs.path)
}

func (fs *FileStorage) LoadState() (int, string, []LogEntry, error) {
	data, err := os.ReadFile(fs.path)
	if err != nil {
		return 0, "", nil, err
	}
	var state persistedState
	if err := json.Unmarshal(data, &state); err != nil {
		return 0, "", nil, err
	}
	return state.CurrentTerm, state.VotedFor, state.Log, nil
}
