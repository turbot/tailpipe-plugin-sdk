package artifact_source

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/turbot/tailpipe-plugin-sdk/collection_state"
	"github.com/turbot/tailpipe-plugin-sdk/context_values"
	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/rate_limiter"
	"github.com/turbot/tailpipe-plugin-sdk/types"
)

// failingDownloadSource discovers one artifact and fails to download it
type failingDownloadSource struct {
	ArtifactSourceImpl[*NilArtifactSourceConfig, *NilConfig]
}

func (s *failingDownloadSource) Identifier() string { return "failing_download" }

func (s *failingDownloadSource) DiscoverArtifacts(ctx context.Context) error {
	return s.OnArtifactDiscovered(ctx, &types.ArtifactInfo{Name: "a.log"})
}

func (s *failingDownloadSource) DownloadArtifact(context.Context, *types.ArtifactInfo) error {
	return fmt.Errorf("download failed")
}

// failingExtractSource discovers and downloads one artifact that cannot be extracted: its file does not exist
type failingExtractSource struct {
	ArtifactSourceImpl[*NilArtifactSourceConfig, *NilConfig]
	localName string
}

func (s *failingExtractSource) Identifier() string { return "failing_extract" }

func (s *failingExtractSource) DiscoverArtifacts(ctx context.Context) error {
	return s.OnArtifactDiscovered(ctx, &types.ArtifactInfo{Name: "a.log"})
}

func (s *failingExtractSource) DownloadArtifact(ctx context.Context, info *types.ArtifactInfo) error {
	return s.OnArtifactDownloaded(ctx, types.NewDownloadedArtifactInfo(info, s.localName, 0))
}

// listingErrorSource discovers one artifact, whose download takes a while and fails, and then fails its listing
type listingErrorSource struct {
	ArtifactSourceImpl[*NilArtifactSourceConfig, *NilConfig]
}

func (s *listingErrorSource) Identifier() string { return "listing_error" }

func (s *listingErrorSource) DiscoverArtifacts(ctx context.Context) error {
	if err := s.OnArtifactDiscovered(ctx, &types.ArtifactInfo{Name: "a.log"}); err != nil {
		return err
	}
	return fmt.Errorf("listing failed")
}

func (s *listingErrorSource) DownloadArtifact(context.Context, *types.ArtifactInfo) error {
	time.Sleep(50 * time.Millisecond)
	return fmt.Errorf("download failed")
}

// collectedState is a collection state that accepts every collected artifact
type collectedState struct {
	collection_state.CollectionState
}

func (collectedState) OnCollected(string, time.Time) error { return nil }

// slowErrorRecorder takes a while to accept an error event, as a GRPC send can
type slowErrorRecorder struct {
	mu     sync.Mutex
	errors []string
}

func (r *slowErrorRecorder) Notify(_ context.Context, e events.Event) error {
	if errEvent, ok := e.(*events.Error); ok {
		time.Sleep(50 * time.Millisecond)
		r.mu.Lock()
		defer r.mu.Unlock()
		r.errors = append(r.errors, errEvent.Err.Error())
	}
	return nil
}

// A download error is sent before Collect returns, so it is reported in its own collection, ahead of the completion
// event that follows Collect, and never in the next collection.
func TestArtifactSourceImpl_DownloadErrorIsSentBeforeCollectReturns(t *testing.T) {
	s := &failingDownloadSource{}
	s.Source = s
	s.artifactDownloadLimiter = rate_limiter.NewAPILimiter(&rate_limiter.Definition{Name: "test", MaxConcurrency: 1})
	r := &slowErrorRecorder{}
	require.NoError(t, s.AddObserver(r))

	require.NoError(t, s.Collect(context_values.WithExecutionId(context.Background(), "exec")))

	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Equal(t, []string{"download failed"}, r.errors)
}

// An extraction error is likewise sent before Collect returns.
func TestArtifactSourceImpl_ExtractionErrorIsSentBeforeCollectReturns(t *testing.T) {
	dir := t.TempDir()
	s := &failingExtractSource{localName: dir + "/missing.log"}
	s.Source = s
	s.artifactDownloadLimiter = rate_limiter.NewAPILimiter(&rate_limiter.Definition{Name: "test", MaxConcurrency: 1})
	state, err := collection_state.NewSaveableCollectionState(collectedState{}, dir+"/state.json")
	require.NoError(t, err)
	s.CollectionState = state
	r := &slowErrorRecorder{}
	require.NoError(t, s.AddObserver(r))

	require.NoError(t, s.Collect(context_values.WithExecutionId(context.Background(), "exec")))

	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Len(t, r.errors, 1)
}

// When the listing fails, Collect still waits for the artifacts it already discovered, so their events are sent before
// the collection completes.
func TestArtifactSourceImpl_ListingErrorWaitsForDiscoveredArtifacts(t *testing.T) {
	s := &listingErrorSource{}
	s.Source = s
	s.artifactDownloadLimiter = rate_limiter.NewAPILimiter(&rate_limiter.Definition{Name: "test", MaxConcurrency: 1})
	r := &slowErrorRecorder{}
	require.NoError(t, s.AddObserver(r))

	err := s.Collect(context_values.WithExecutionId(context.Background(), "exec"))
	require.EqualError(t, err, "listing failed")

	r.mu.Lock()
	defer r.mu.Unlock()
	assert.Equal(t, []string{"download failed"}, r.errors)
}
