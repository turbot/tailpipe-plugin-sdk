package plugin

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/turbot/tailpipe-plugin-sdk/events"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/proto"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
)

// recorder is an observer that keeps every event it is sent
type recorder struct {
	mu     sync.Mutex
	events []events.Event
}

func (r *recorder) Notify(_ context.Context, e events.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, e)
	return nil
}

func (r *recorder) executionIds() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	ids := make([]string, len(r.events))
	for i, e := range r.events {
		ids[i] = e.(*events.Started).ExecutionId
	}
	return ids
}

func TestPluginImpl_EventsBeforeObserverAreSentToIt(t *testing.T) {
	p := NewPluginImpl("test")
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 100; i++ {
		require.NoError(t, p.Notify(ctx, events.NewStartedEvent(fmt.Sprint(i))))
	}
	// the context the events were sent with has ended before the observer attaches
	cancel()

	r := &recorder{}
	require.NoError(t, p.AddObserver(r))
	require.NoError(t, p.Notify(context.Background(), events.NewStartedEvent("100")))

	want := make([]string, 101)
	for i := range want {
		want[i] = fmt.Sprint(i)
	}
	assert.Equal(t, want, r.executionIds())
}

func TestPluginImpl_ErrorBeforeObserverIsSentToIt(t *testing.T) {
	p := NewPluginImpl("test")
	p.NotifyError(context.Background(), "exec", fmt.Errorf("boom"))

	r := &recorder{}
	require.NoError(t, p.AddObserver(r))

	require.Len(t, r.events, 1)
	e, ok := r.events[0].(*events.Error)
	require.True(t, ok)
	assert.Equal(t, "boom", e.Err.Error())
}

func TestPluginImpl_OnlyTheFirstObserverGetsHeldEvents(t *testing.T) {
	p := NewPluginImpl("test")
	require.NoError(t, p.Notify(context.Background(), events.NewStartedEvent("held")))

	first, second := &recorder{}, &recorder{}
	require.NoError(t, p.AddObserver(first))
	require.NoError(t, p.AddObserver(second))
	require.NoError(t, p.Notify(context.Background(), events.NewStartedEvent("after")))

	assert.Equal(t, []string{"held", "after"}, first.executionIds())
	assert.Equal(t, []string{"after"}, second.executionIds())
}

// Events sent from several goroutines while the observer attaches each reach it once, in each sender's order.
func TestPluginImpl_EventsSentWhileObserverAttaches(t *testing.T) {
	for run := 0; run < 50; run++ {
		p := NewPluginImpl("test")
		const senders, perSender = 4, 200
		var wg sync.WaitGroup
		start := make(chan struct{})
		for s := 0; s < senders; s++ {
			wg.Add(1)
			go func(s int) {
				defer wg.Done()
				<-start
				for i := 0; i < perSender; i++ {
					_ = p.Notify(context.Background(), events.NewStartedEvent(fmt.Sprintf("%d-%d", s, i)))
				}
			}(s)
		}
		r := &recorder{}
		close(start)
		require.NoError(t, p.AddObserver(r))
		wg.Wait()

		ids := r.executionIds()
		require.Len(t, ids, senders*perSender, "run %d", run)
		next := make([]int, senders)
		for _, id := range ids {
			var s, i int
			_, err := fmt.Sscanf(id, "%d-%d", &s, &i)
			require.NoError(t, err)
			require.Equal(t, next[s], i, "run %d: sender %d's events out of order or repeated", run, s)
			next[s]++
		}
	}
}

// eventPlugin is the smallest TailpipePlugin: the common implementation, and a Describe
type eventPlugin struct {
	PluginImpl
}

func (p *eventPlugin) Describe(context.Context, *proto.DescribeRequest) (*proto.DescribeResponse, error) {
	return &proto.DescribeResponse{}, nil
}

// Over a real GRPC stream: the caller opens the event stream and the plugin sends its events at once, as a source
// plugin does when the calling plugin starts the collection straight after opening the stream. Every event arrives.
func TestPluginServer_EventsSentAsTheStreamOpensArrive(t *testing.T) {
	for run := 0; run < 50; run++ {
		impl := &eventPlugin{PluginImpl: NewPluginImpl("test")}

		listener := bufconn.Listen(1 << 20)
		server := grpc.NewServer()
		proto.RegisterTailpipePluginServer(server, shared.TailpipePluginServerWrapper{Impl: PluginServer{impl: impl}})
		go func() { _ = server.Serve(listener) }()

		conn, err := grpc.NewClient("passthrough:///bufnet",
			grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
			grpc.WithTransportCredentials(insecure.NewCredentials()))
		require.NoError(t, err)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		stream, err := proto.NewTailpipePluginClient(conn).AddObserver(ctx, &proto.Empty{})
		require.NoError(t, err)

		const n = 20
		for i := 0; i < n; i++ {
			require.NoError(t, impl.Notify(ctx, events.NewStartedEvent(fmt.Sprint(i))))
		}
		require.NoError(t, impl.Notify(ctx, events.NewSourceCompleteEvent("exec", nil)))

		for i := 0; i < n; i++ {
			e, err := stream.Recv()
			require.NoError(t, err, "run %d: event %d", run, i)
			require.Equal(t, fmt.Sprint(i), e.GetStartedEvent().GetExecutionId(), "run %d", run)
		}
		e, err := stream.Recv()
		require.NoError(t, err, "run %d: the completion event", run)
		require.NotNil(t, e.GetSourceCompleteEvent(), "run %d", run)

		cancel()
		_ = conn.Close()
		server.Stop()
	}
}
