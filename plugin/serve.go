package plugin

import (
	"context"
	"fmt"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-plugin"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
	"github.com/turbot/tailpipe-plugin-sdk/logging"
	"google.golang.org/grpc"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
)

// ServeOpts are the configurations to serve a plugin.
type ServeOpts struct {
	// todo will we need a ctor rather than an instance? will we have some form if 'dynamic' plugin?
	//PluginFunc PluginFunc
	Plugin TailpipePlugin
}

type PluginFunc func(context.Context) TailpipePlugin

const (
	UnrecognizedRemotePluginMessage       = "Unrecognized remote plugin message:"
	UnrecognizedRemotePluginMessageSuffix = "\nThis usually means"
	PluginStartupFailureMessage           = "Plugin startup failed: "
)

// Serve creates and starts the GRPC server which serves the plugin,
//
//	It is called from the main function of the plugin.
func Serve(opts *ServeOpts) error {
	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("%s%s", PluginStartupFailureMessage, helpers.ToError(r).Error())
			// write to stdout so the plugin manager can extract the error message
			fmt.Println(msg)
		}
	}()

	// retrieve the plugin from the opts
	p := opts.Plugin

	// initialize logger
	logging.Initialize(p.Identifier())

	slog.Info("Serve")

	// write to stderr
	fmt.Fprintf(os.Stderr, " TEST______")

	// initialise the plugin - create the connection config map, set plugin pointer on all tables
	ctx := context.Background()
	if err := p.Init(ctx); err != nil {
		return err
	}
	// shutdown the plugin when done
	defer p.Shutdown(ctx)

	if _, found := os.LookupEnv("TAILPIPE_PPROF"); found {
		setupPprof()
	}

	pluginMap := map[string]plugin.Plugin{
		p.Identifier(): &shared.TailpipeGRPCPlugin{Impl: p},
	}
	plugin.Serve(&plugin.ServeConfig{
		Plugins:         pluginMap,
		GRPCServer:      newGRPCServer,
		HandshakeConfig: shared.Handshake,
		// disable server logging
		Logger: hclog.New(&hclog.LoggerOptions{Level: hclog.Off}),
	})
	return nil
}

func newGRPCServer(options []grpc.ServerOption) *grpc.Server {
	// set the buffer size to 10Mb
	//options = append(options, grpc.MaxRecvMsgSize(10*1024*1024))
	//options = append(options, grpc.MaxSendMsgSize(40*1024*1024))
	// set the write buffer size to 512 K
	//options = append(options, grpc.WriteBufferSize(512*1024))
	//// set the read buffer size to 512 K
	//options = append(options, grpc.ReadBufferSize(512*1024))
	return grpc.NewServer(options...)
}

func setupPprof() {
	slog.Info("PROFILING!!!!")
	go func() {
		listener, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			slog.Error("Error starting pprof", "error", err)
			return
		}
		slog.Info("Check http://localhost:%d/debug/pprof/", listener.Addr().(*net.TCPAddr).Port)
		err = http.Serve(listener, nil)
		if err != nil {
			slog.Error("Error starting pprof", "error", err)
		}
	}()
}
