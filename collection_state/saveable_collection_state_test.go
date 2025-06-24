package collection_state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestTimeRangeCollectionState_migrate(t1 *testing.T) {
	tests := []struct {
		name               string
		source             any
		newCollectionState func() CollectionState
		expectedState      *TimeRangeCollectionState
		expectError        bool
	}{
		{
			name: "migrate ReverseOrderCollectionState",
			source: &ReverseOrderCollectionStateLegacy{
				TimeRanges: []*TimeRangeCollectionStateLegacy{
					buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
				},
			},
			newCollectionState: NewTimeRangeCollectionState,
			expectedState: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*24,
				buildTimeRangeState("2023-12-01 01:00:00", "2023-10-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object1", "object2"),
			),
		},
		{
			name: "migrate TimeRangeCollectionStateLegacy",
			source: &TimeRangeCollectionStateLegacy{
				FirstEntryTime:  timeString("2023-10-01 00:00:00"),
				LastEntryTime:   timeString("2023-12-01 01:00:00"),
				EndTime:         timeString("2023-11-30 01:00:00"),
				EndObjects:      map[string]struct{}{"object1": {}, "object2": {}},
				Granularity:     time.Hour * 12,
				CollectionOrder: CollectionOrderChronological,
			},
			newCollectionState: NewTimeRangeCollectionState,
			expectedState: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*12,
				buildTimeRangeState("2023-10-01 00:00:00", "2023-11-30 01:00:00", time.Hour*12, CollectionOrderChronological, "object1", "object2"),
			),
		},
		{
			name: "migrate multiple ranges",
			source: &ReverseOrderCollectionStateLegacy{
				TimeRanges: []*TimeRangeCollectionStateLegacy{
					buildTimeRangeCollectionStateLegacy("2023-10-01 00:00:00", "2023-11-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object1"),
					buildTimeRangeCollectionStateLegacy("2023-11-01 00:00:00", "2023-12-01 01:00:00", time.Hour*24, CollectionOrderReverse, "object2"),
				},
			},
			newCollectionState: NewTimeRangeCollectionState,
			expectedState: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*24,
				buildTimeRangeState("2023-11-01 00:00:00", "2023-10-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object1"),
				buildTimeRangeState("2023-12-01 01:00:00", "2023-11-01 00:00:00", time.Hour*24, CollectionOrderReverse, "object2"),
			),
		},
		{
			name: "invalid legacy format should result in empty state",
			source: map[string]interface{}{
				"invalid_field": "invalid_value",
			},
			expectedState:      buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*24),
			newCollectionState: NewTimeRangeCollectionState,
			expectError:        false,
		},
	}

	for _, tt := range tests {
		t1.Run(tt.name, func(t1 *testing.T) {
			// Serialize the source state to JSON
			sourceJSON, err := json.Marshal(tt.source)
			if err != nil {
				t1.Fatalf("failed to marshal source state: %v", err)
			}

			// Write the JSON to a temp file
			tmpDir := t1.TempDir()
			tmpFile := filepath.Join(tmpDir, "collection_state.json")
			//nolint:gosec // test code
			err = os.WriteFile(tmpFile, sourceJSON, 0644)
			if err != nil {
				t1.Fatalf("failed to write temp file: %v", err)
			}

			// Create a new saveable collection state
			saveableState, _ := NewSaveableCollectionState(tt.newCollectionState(), "")

			// Test just the migration functionality
			err = saveableState.LoadFromFile(tmpFile)

			// Check error expectations
			if tt.expectError {
				if err == nil {
					t1.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t1.Fatalf("unexpected error during migration: %v", err)
			}

			// Compare the migrated state with expected
			state := saveableState.State.(*TimeRangeCollectionState)
			equal, diff := state.Compare(tt.expectedState)
			if !equal {
				t1.Errorf("migrated state does not match expected: %s", diff)
			}
		})
	}
}

func TestSaveableCollectionState_SaveAndLoad(t *testing.T) {
	tests := []struct {
		name      string
		state     CollectionState
		setupFile func(string) error
	}{
		{
			name: "save and load TimeRangeCollectionState",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*24,
				buildTimeRangeState("2023-10-01 00:00:00", "2023-11-01 00:00:00", time.Hour*24, CollectionOrderChronological, "object1", "object2"),
			),
		},
		{
			name: "save and load reverse order state",
			state: buildTimeRangeCollectionState(CollectionOrderReverse, time.Hour*12,
				buildTimeRangeState("2023-12-01 01:00:00", "2023-10-01 00:00:00", time.Hour*12, CollectionOrderReverse, "object1"),
			),
		},
		{
			name: "save and load multiple ranges",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*6,
				buildTimeRangeState("2023-10-01 00:00:00", "2023-10-15 00:00:00", time.Hour*6, CollectionOrderChronological, "object1"),
				buildTimeRangeState("2023-10-15 00:00:00", "2023-11-01 00:00:00", time.Hour*6, CollectionOrderChronological, "object2"),
			),
		},
		{
			name:  "save empty state should delete file",
			state: buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*24),
		},
		{
			name: "load non-existent file should fail",
			setupFile: func(path string) error {
				// Don't create any file
				return nil
			},
		},
		{
			name: "load invalid JSON should fail",
			setupFile: func(path string) error {
				//nolint:gosec // test code
				return os.WriteFile(path, []byte("invalid json"), 0644)
			},
		},
		{
			name: "load empty file should fail",
			setupFile: func(path string) error {
				//nolint:gosec // test code
				return os.WriteFile(path, []byte(""), 0644)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create temp directory and file path
			tmpDir := t.TempDir()
			tmpFile := filepath.Join(tmpDir, "collection_state.json")

			// Setup file according to test
			if tt.setupFile != nil {
				if err := tt.setupFile(tmpFile); err != nil {
					t.Fatalf("failed to setup test file: %v", err)
				}
			}

			// Test Save (only if we have a state to save)
			if tt.state != nil {
				saveableState, _ := NewSaveableCollectionState(tt.state, "")
				saveableState.jsonPath = tmpFile
				err := saveableState.Save()
				if err != nil {
					t.Fatalf("unexpected error during save: %v", err)
				}

				// Check if file was created/deleted as expected
				fileInfo, err := os.Stat(tmpFile)
				if !tt.state.IsEmpty() {
					if err != nil {
						t.Fatalf("expected file to exist but got error: %v", err)
					}
					if fileInfo.Size() == 0 {
						t.Errorf("expected file to have content but size is 0")
					}
				} else if err == nil {
					// For empty states, file should be deleted
					t.Errorf("expected file to be deleted but it still exists")
				}
			}

			// Test Load (if we have setupFile or if state is not empty)
			shouldTestLoad := tt.setupFile != nil || (tt.state != nil && !tt.state.IsEmpty())
			if shouldTestLoad {
				// Create a new saveable state to test loading
				newSaveableState, _ := NewSaveableCollectionState(NewTimeRangeCollectionState(), "")
				newSaveableState.jsonPath = tmpFile
				err := newSaveableState.LoadFromFile(tmpFile)
				if tt.setupFile != nil {
					if err == nil {
						t.Errorf("expected error but got none")
					}
					return
				}
				if err != nil {
					t.Fatalf("unexpected error during load: %v", err)
				}

				// Compare the loaded state with original (only for successful loads)
				if tt.state != nil {
					loadedState := newSaveableState.State.(*TimeRangeCollectionState)
					originalState := tt.state.(*TimeRangeCollectionState)
					equal, diff := loadedState.Compare(originalState)
					if !equal {
						t.Errorf("loaded state does not match original: %s", diff)
					}

					// Check that StructVersion was set correctly
					if newSaveableState.StructVersion != CollectionStateStructVersion {
						t.Errorf("expected StructVersion to be %d, got %d", CollectionStateStructVersion, newSaveableState.StructVersion)
					}
				}
			}
		})
	}
}

// TestSaveableCollectionState_SaveOptimization verifies that calling Save on a SaveableCollectionState
// does not rewrite the file if the state has not changed, by checking that the file modification time
// remains the same after consecutive Save calls with no intervening changes.
func TestSaveableCollectionState_SaveOptimization(t *testing.T) {
	// Test that Save doesn't write to file if nothing has changed
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "collection_state.json")

	// Create initial state
	initialState := buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*24,
		buildTimeRangeState("2023-10-01 00:00:00", "2023-11-01 00:00:00", time.Hour*24, CollectionOrderChronological, "object1"),
	)

	saveableState, _ := NewSaveableCollectionState(initialState, "")
	saveableState.jsonPath = tmpFile

	// First save
	err := saveableState.Save()
	if err != nil {
		t.Fatalf("unexpected error during first save: %v", err)
	}

	// Get file info after first save
	fileInfo1, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("failed to get file info after first save: %v", err)
	}
	modTime1 := fileInfo1.ModTime()

	// Wait a bit to ensure time difference
	time.Sleep(10 * time.Millisecond)

	// Second save without any changes
	err = saveableState.Save()
	if err != nil {
		t.Fatalf("unexpected error during second save: %v", err)
	}

	// Get file info after second save
	fileInfo2, err := os.Stat(tmpFile)
	if err != nil {
		t.Fatalf("failed to get file info after second save: %v", err)
	}
	modTime2 := fileInfo2.ModTime()

	// File modification time should not have changed since nothing was modified
	if !modTime1.Equal(modTime2) {
		t.Errorf("file modification time changed between saves, expected no change. First: %v, Second: %v", modTime1, modTime2)
	}
}

func TestSaveableCollectionState_LoadWithLegacyMigration(t *testing.T) {
	// Test loading a legacy state file
	tmpDir := t.TempDir()
	tmpFile := filepath.Join(tmpDir, "collection_state.json")

	// Create legacy state JSON
	legacyState := &TimeRangeCollectionStateLegacy{
		FirstEntryTime:  timeString("2023-10-01 00:00:00"),
		LastEntryTime:   timeString("2023-12-01 01:00:00"),
		EndTime:         timeString("2023-11-30 01:00:00"),
		EndObjects:      map[string]struct{}{"object1": {}, "object2": {}},
		Granularity:     time.Hour * 12,
		CollectionOrder: CollectionOrderChronological,
	}

	legacyJSON, err := json.Marshal(legacyState)
	if err != nil {
		t.Fatalf("failed to marshal legacy state: %v", err)
	}

	// Write legacy JSON to file
	//nolint:gosec // test code
	err = os.WriteFile(tmpFile, legacyJSON, 0644)
	if err != nil {
		t.Fatalf("failed to write legacy file: %v", err)
	}

	// Create saveable state and test load
	state, _ := NewSaveableCollectionState(NewTimeRangeCollectionState(), "")
	err = state.LoadFromFile(tmpFile)
	if err != nil {
		t.Fatalf("unexpected error during legacy load: %v", err)
	}

	// Verify the state was migrated correctly
	expectedState := buildTimeRangeCollectionState(CollectionOrderChronological, time.Hour*12,
		buildTimeRangeState("2023-10-01 00:00:00", "2023-11-30 01:00:00", time.Hour*12, CollectionOrderChronological, "object1", "object2"),
	)

	loadedState := state.State.(*TimeRangeCollectionState)
	equal, diff := loadedState.Compare(expectedState)
	if !equal {
		t.Errorf("migrated state does not match expected: %s", diff)
	}
}

// buildSaveableCollectionState constructs a SaveableCollectionState for tests
func buildSaveableCollectionState(state CollectionState, jsonPath string) *SaveableCollectionState {
	saveableState, _ := NewSaveableCollectionState(state, "")
	saveableState.jsonPath = jsonPath
	return saveableState
}

// New test for SaveableCollectionState error cases
func TestSaveableCollectionState_SaveErrors(t *testing.T) {
	tests := []struct {
		name          string
		saveableState *SaveableCollectionState
		expectError   bool
	}{
		{
			name:          "save without path should fail",
			saveableState: buildSaveableCollectionState(NewTimeRangeCollectionState(), ""),
			expectError:   true,
		},
		{
			name:          "save with invalid path should succeed (os.WriteFile creates directories)",
			saveableState: buildSaveableCollectionState(NewTimeRangeCollectionState(), "/invalid/path/that/does/not/exist/collection_state.json"),
			expectError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.saveableState.Save()
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error but got none")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error during save: %v", err)
			}
		})
	}
}
