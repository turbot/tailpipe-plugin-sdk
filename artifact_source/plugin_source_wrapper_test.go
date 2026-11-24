package artifact_source

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	googlegrpc "google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/turbot/tailpipe-plugin-sdk/grpc"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
)

// pauseRecorder is a source plugin that records the pause and resume calls it gets
type pauseRecorder struct {
	proto.UnimplementedTailpipePluginServer
	mu    sync.Mutex
	calls []string
}

func (r *pauseRecorder) record(call string) (*proto.Empty, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, call)
	return &proto.Empty{}, nil
}

func (r *pauseRecorder) SourcePause(context.Context, *proto.Empty) (*proto.Empty, error) {
	return r.record("pause")
}

func (r *pauseRecorder) SourceResume(context.Context, *proto.Empty) (*proto.Empty, error) {
	return r.record("resume")
}

// A paused source in another plugin is resumed: Resume sends the source plugin a resume, not a second pause.
func TestPluginSourceWrapper_ResumeResumesTheSourcePlugin(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	server := googlegrpc.NewServer()
	sourcePlugin := &pauseRecorder{}
	proto.RegisterTailpipePluginServer(server, sourcePlugin)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := googlegrpc.NewClient("passthrough:///bufnet",
		googlegrpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		googlegrpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	raw, err := (&shared.TailpipeGRPCPlugin{}).GRPCClient(context.Background(), nil, conn)
	require.NoError(t, err)
	w := &PluginSourceWrapper{
		client: &grpc.PluginClient{TailpipePluginClientWrapper: *raw.(*shared.TailpipePluginClientWrapper)},
	}

	require.NoError(t, w.Pause())
	require.NoError(t, w.Resume())

	assert.Equal(t, []string{"pause", "resume"}, sourcePlugin.calls)
}
