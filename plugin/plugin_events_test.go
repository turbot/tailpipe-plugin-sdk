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

// recorder is an observer that keeps a label for every event it is sent
type recorder struct {
	mu     sync.Mutex
	labels []string
}

func (r *recorder) Notify(_ context.Context, e events.Event) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.labels = append(r.labels, label(e))
	return nil
}

func (r *recorder) got() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.labels...)
}

func label(e events.Event) string {
	switch e := e.(type) {
	case *events.Started:
		return e.ExecutionId
	case *events.Complete:
		return "complete " + e.ExecutionId
	case *events.SourceComplete:
		return "source complete " + e.ExecutionId
	case *events.Error:
		return "error " + e.Err.Error()
	}
	return fmt.Sprintf("%T", e)
}

func started(id string) events.Event        { return events.NewStartedEvent(id) }
func complete(id string) events.Event       { return events.NewCompletedEvent(id, 0, 0, nil) }
func sourceComplete(id string) events.Event { return events.NewSourceCompleteEvent(id, nil) }

func notify(t *testing.T, p *PluginImpl, evs ...events.Event) {
	t.Helper()
	for _, e := range evs {
		require.NoError(t, p.Notify(context.Background(), e))
	}
}

func TestPluginImpl_EventsBeforeStreamAreSentToIt(t *testing.T) {
	p := NewPluginImpl("test")
	ctx, cancel := context.WithCancel(context.Background())
	for i := 0; i < 100; i++ {
		require.NoError(t, p.Notify(ctx, started(fmt.Sprint(i))))
	}
	// the context the events were sent with has ended before the stream attaches
	cancel()

	r := &recorder{}
	require.NoError(t, p.AddObserver(r))
	notify(t, &p, started("100"))

	want := make([]string, 101)
	for i := range want {
		want[i] = fmt.Sprint(i)
	}
	assert.Equal(t, want, r.got())
}

func TestPluginImpl_ErrorBeforeStreamIsSentToIt(t *testing.T) {
	p := NewPluginImpl("test")
	p.NotifyError(context.Background(), "exec", fmt.Errorf("boom"))

	r := &recorder{}
	require.NoError(t, p.AddObserver(r))

	assert.Equal(t, []string{"error boom"}, r.got())
}

// A new stream replaces the one before it: later events go to the new stream only.
func TestPluginImpl_NewStreamReplacesTheOld(t *testing.T) {
	p := NewPluginImpl("test")
	first, second := &recorder{}, &recorder{}
	require.NoError(t, p.AddObserver(first))
	notify(t, &p, started("a"))
	require.NoError(t, p.AddObserver(second))
	notify(t, &p, started("b"))

	assert.Equal(t, []string{"a"}, first.got())
	assert.Equal(t, []string{"b"}, second.got())
}

// The CLI opens a stream for each collection and stops reading it after the collection's completion event. The next
// collection's events, sent before or after its own stream attaches, reach that stream and never the spent one.
// The same holds for a plugin used as a source by another plugin: a stream for each source collection, read until
// its source completion event.
func TestPluginImpl_SecondCollectionReachesItsOwnStream(t *testing.T) {
	for _, end := range []func(string) events.Event{complete, sourceComplete} {
		for _, streamFirst := range []bool{true, false} {
			name := fmt.Sprintf("%s, stream attaches first=%v", label(end("")), streamFirst)
			t.Run(name, func(t *testing.T) {
				p := NewPluginImpl("test")
				first, second := &recorder{}, &recorder{}
				require.NoError(t, p.AddObserver(first))
				notify(t, &p, started("p1"), end("p1"))

				if streamFirst {
					require.NoError(t, p.AddObserver(second))
					notify(t, &p, started("p2"), end("p2"))
				} else {
					notify(t, &p, started("p2"), end("p2"))
					require.NoError(t, p.AddObserver(second))
				}

				assert.Equal(t, []string{"p1", label(end("p1"))}, first.got())
				assert.Equal(t, []string{"p2", label(end("p2"))}, second.got())
			})
		}
	}
}

// A collection that ends before its stream attaches: the stream gets that collection's events, completion included,
// and is then retired, so what follows waits for the next stream.
func TestPluginImpl_HeldCompletionRetiresTheStream(t *testing.T) {
	p := NewPluginImpl("test")
	notify(t, &p, started("p1"), complete("p1"), started("p2"))

	first, second := &recorder{}, &recorder{}
	require.NoError(t, p.AddObserver(first))
	assert.Equal(t, []string{"p1", "complete p1"}, first.got())

	require.NoError(t, p.AddObserver(second))
	notify(t, &p, complete("p2"))
	assert.Equal(t, []string{"p2", "complete p2"}, second.got())
	assert.Equal(t, []string{"p1", "complete p1"}, first.got())
}

// When Collect fails before a collection starts, no completion event will come, and the caller stops reading the
// stream it opened. The next collection's events wait for the next stream instead of going to that one.
func TestPluginImpl_FailedCollectRetiresTheStream(t *testing.T) {
	p := NewPluginImpl("test")
	first, second := &recorder{}, &recorder{}
	require.NoError(t, p.AddObserver(first))
	p.collectionFailed()

	notify(t, &p, started("p2"))
	require.NoError(t, p.AddObserver(second))
	notify(t, &p, complete("p2"))

	assert.Empty(t, first.got())
	assert.Equal(t, []string{"p2", "complete p2"}, second.got())
}

// When Collect fails before its stream has attached, that stream is not used when it attaches: the next stream gets
// the next collection's events.
func TestPluginImpl_StreamAttachingAfterItsCollectFailedIsNotUsed(t *testing.T) {
	p := NewPluginImpl("test")
	first, second := &recorder{}, &recorder{}
	p.collectionFailed()
	require.NoError(t, p.AddObserver(first))

	notify(t, &p, started("p2"))
	require.NoError(t, p.AddObserver(second))
	notify(t, &p, complete("p2"))

	assert.Empty(t, first.got())
	assert.Equal(t, []string{"p2", "complete p2"}, second.got())
}

// failingObserver is a stream whose sends fail, as when the caller's connection has gone
type failingObserver struct{}

func (failingObserver) Notify(context.Context, events.Event) error {
	return fmt.Errorf("transport is closing")
}

// A source collection whose completion event fails to send: the stream is retired by that event, and the failure that
// SourceCollect then returns does not leave the next caller's stream unused.
func TestPluginImpl_FailedSendOfCompletionKeepsTheNextStream(t *testing.T) {
	p := NewPluginImpl("test")
	require.NoError(t, p.AddObserver(failingObserver{}))
	p.collectionBegins()
	require.Error(t, p.Notify(context.Background(), sourceComplete("p1")))
	p.collectionFailed()

	second := &recorder{}
	require.NoError(t, p.AddObserver(second))
	p.collectionBegins()
	notify(t, &p, sourceComplete("p2"))
	assert.Equal(t, []string{"source complete p2"}, second.got())
}

// A Collect that fails after its collection completed (and retired its stream) does not leave the next stream unused.
func TestPluginImpl_FailedCollectAfterItsCompletionKeepsTheNextStream(t *testing.T) {
	p := NewPluginImpl("test")
	first, second := &recorder{}, &recorder{}
	require.NoError(t, p.AddObserver(first))
	p.collectionBegins()
	p.collectionStarted()
	notify(t, &p, started("p1"), complete("p1"))
	p.collectionFailed()

	require.NoError(t, p.AddObserver(second))
	p.collectionBegins()
	notify(t, &p, started("p2"), complete("p2"))
	assert.Equal(t, []string{"p1", "complete p1"}, first.got())
	assert.Equal(t, []string{"p2", "complete p2"}, second.got())
}

// When Collect fails after its collection has started, that collection's completion event still retires the stream.
func TestPluginImpl_FailedCollectWithRunningCollectionKeepsTheStream(t *testing.T) {
	p := NewPluginImpl("test")
	first := &recorder{}
	require.NoError(t, p.AddObserver(first))
	p.collectionStarted()
	p.collectionFailed()

	notify(t, &p, complete("p1"))
	assert.Equal(t, []string{"complete p1"}, first.got())
}

// Senders that sent before the stream attached and go on sending while it attaches: every event arrives once, in
// each sender's order, with the held events first.
func TestPluginImpl_EventsSentWhileStreamAttaches(t *testing.T) {
	for run := 0; run < 50; run++ {
		p := NewPluginImpl("test")
		const senders, perSender = 4, 200
		var sentHalf, done sync.WaitGroup
		attach := make(chan struct{})
		for s := 0; s < senders; s++ {
			sentHalf.Add(1)
			done.Add(1)
			go func(s int) {
				defer done.Done()
				for i := 0; i < perSender; i++ {
					if i == perSender/2 {
						sentHalf.Done()
						<-attach
					}
					_ = p.Notify(context.Background(), started(fmt.Sprintf("%d-%d", s, i)))
				}
			}(s)
		}
		// every sender has sent half its events with no stream; the rest race the stream attaching
		sentHalf.Wait()
		r := &recorder{}
		close(attach)
		require.NoError(t, p.AddObserver(r))
		done.Wait()

		got := r.got()
		require.Len(t, got, senders*perSender, "run %d", run)
		next := make([]int, senders)
		for _, id := range got {
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

// servePlugin serves the plugin over an in-memory GRPC connection and returns a client for it
func servePlugin(t *testing.T, impl TailpipePlugin) proto.TailpipePluginClient {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	proto.RegisterTailpipePluginServer(server, shared.TailpipePluginServerWrapper{Impl: PluginServer{impl: impl}})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return proto.NewTailpipePluginClient(conn)
}

func receive(t *testing.T, stream proto.TailpipePlugin_AddObserverClient, run int, want ...string) {
	t.Helper()
	for i, w := range want {
		e, err := stream.Recv()
		require.NoError(t, err, "run %d: event %d (%s) never arrived", run, i, w)
		got := e.GetStartedEvent().GetExecutionId()
		if c := e.GetCompleteEvent(); c != nil {
			got = "complete " + c.GetExecutionId()
		}
		if c := e.GetSourceCompleteEvent(); c != nil {
			got = "source complete " + c.GetExecutionId()
		}
		require.Equal(t, w, got, "run %d: event %d", run, i)
	}
}

// Over a real GRPC stream: the caller opens the event stream and the plugin sends its events at once, as a source
// plugin does when the calling plugin starts the collection straight after opening the stream. Every event arrives.
func TestPluginServer_EventsSentAsTheStreamOpensArrive(t *testing.T) {
	for run := 0; run < 50; run++ {
		impl := &eventPlugin{PluginImpl: NewPluginImpl("test")}
		client := servePlugin(t, impl)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		stream, err := client.AddObserver(ctx, &proto.Empty{})
		require.NoError(t, err)
		var want []string
		for i := 0; i < 20; i++ {
			require.NoError(t, impl.Notify(ctx, started(fmt.Sprint(i))))
			want = append(want, fmt.Sprint(i))
		}
		require.NoError(t, impl.Notify(ctx, complete("exec")))

		receive(t, stream, run, append(want, "complete exec")...)
		cancel()
	}
}

// Over a real GRPC stream, as a `tailpipe collect` of two partitions on one plugin: a stream for each collection,
// the first left unread after its completion event, and the second collection's events sent as its stream opens.
// The same as a plugin used as a source for two partitions: the CLI keeps the source plugin running, and the calling
// plugin opens a stream for each source collection and stops reading it after the source completion event.
func TestPluginServer_SecondCollectionOnOnePlugin(t *testing.T) {
	for _, end := range []func(string) events.Event{complete, sourceComplete} {
		for run := 0; run < 50; run++ {
			impl := &eventPlugin{PluginImpl: NewPluginImpl("test")}
			client := servePlugin(t, impl)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

			first, err := client.AddObserver(ctx, &proto.Empty{})
			require.NoError(t, err)
			notify(t, &impl.PluginImpl, started("p1"), end("p1"))
			receive(t, first, run, "p1", label(end("p1")))

			second, err := client.AddObserver(ctx, &proto.Empty{})
			require.NoError(t, err)
			notify(t, &impl.PluginImpl, started("p2"), end("p2"))
			receive(t, second, run, "p2", label(end("p2")))
			cancel()
		}
	}
}

// waitForStream waits until the plugin has a stream attached
func waitForStream(t *testing.T, p *PluginImpl) {
	t.Helper()
	waitFor(t, p, func() bool { return p.hasStream })
}

// waitFor waits until the condition, read under the plugin's event lock, holds
func waitFor(t *testing.T, p *PluginImpl, condition func() bool) {
	t.Helper()
	require.Eventually(t, func() bool {
		p.eventLock.Lock()
		defer p.eventLock.Unlock()
		return condition()
	}, 10*time.Second, time.Millisecond)
}

// Over a real GRPC stream: a Collect that fails (an unknown table) leaves its stream unread, and the next collection's
// events, sent as its stream opens, reach the next stream. (The failed collection's stream attaching only after the
// failure is TestPluginImpl_StreamAttachingAfterItsCollectFailedIsNotUsed.)
func TestPluginServer_CollectionAfterAFailedCollect(t *testing.T) {
	for run := 0; run < 50; run++ {
		impl := &eventPlugin{PluginImpl: NewPluginImpl("test")}
		client := servePlugin(t, impl)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		_, err := client.AddObserver(ctx, &proto.Empty{})
		require.NoError(t, err)
		waitForStream(t, &impl.PluginImpl)
		_, err = client.Collect(ctx, &proto.CollectRequest{TableName: "no_such_table", ExecutionId: "p1"})
		require.Error(t, err)

		second, err := client.AddObserver(ctx, &proto.Empty{})
		require.NoError(t, err)
		notify(t, &impl.PluginImpl, started("p2"), complete("p2"))
		receive(t, second, run, "p2", "complete p2")
		cancel()
	}
}

// Over a real GRPC stream: a collection completes, the next Collect fails before its stream attaches, and that stream
// then attaches late. It is left unused, and the collection after gets its events on its own stream.
func TestPluginServer_LateStreamOfAFailedCollectAfterACompletedOne(t *testing.T) {
	for run := 0; run < 20; run++ {
		impl := &eventPlugin{PluginImpl: NewPluginImpl("test")}
		client := servePlugin(t, impl)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)

		first, err := client.AddObserver(ctx, &proto.Empty{})
		require.NoError(t, err)
		waitForStream(t, &impl.PluginImpl)
		notify(t, &impl.PluginImpl, started("p1"), complete("p1"))
		receive(t, first, run, "p1", "complete p1")

		_, err = client.Collect(ctx, &proto.CollectRequest{TableName: "no_such_table", ExecutionId: "p2"})
		require.Error(t, err)
		// the failed collection's stream, attaching only now
		_, err = client.AddObserver(ctx, &proto.Empty{})
		require.NoError(t, err)
		waitFor(t, &impl.PluginImpl, func() bool { return impl.spentStreamsToCome == 0 })

		third, err := client.AddObserver(ctx, &proto.Empty{})
		require.NoError(t, err)
		notify(t, &impl.PluginImpl, started("p3"), complete("p3"))
		receive(t, third, run, "p3", "complete p3")
		cancel()
	}
}
