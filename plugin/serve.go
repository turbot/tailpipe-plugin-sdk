package plugin

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"

	"github.com/hashicorp/go-plugin"
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-sdk/grpc/shared"
)

// ServeOpts are the configurations to serve a plugin.
type ServeOpts struct {
	PluginFunc PluginFunc
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
			log.Println("[WARN]", msg)
			// write to stdout so the plugin manager can extract the error message
			fmt.Println(msg)
		}
	}()

	// create the logger
	//logger := setupLogger()

	slog.Info("Serve")

	// call plugin function to build a plugin object
	ctx := context.Background()
	p := opts.PluginFunc(ctx)

	// initialise the plugin - create the connection config map, set plugin pointer on all tables
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
	log.Printf("[INFO] PROFILING!!!!")
	go func() {
		listener, err := net.Listen("tcp", "localhost:0")
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("[INFO] Check http://localhost:%d/debug/pprof/", listener.Addr().(*net.TCPAddr).Port)
		log.Println(http.Serve(listener, nil))
	}()
}

//
//func setupLogger() hclog.Logger {
//	//
//	// go-plugin reads stderr output line-by-line from the plugin instances and sets the level
//	// based on the prefix. If there's no level in the prefix, it will set it to a default log level of DEBUG
//	// this is a problem for log lines containing "\n", since every line but the first become DEBUG
//	// log instead of being part of the actual log line
//	//
//	// We are using a custom writer here which intercepts the log lines and adds an extra escape to "\n" characters
//	//
//	// The plugin manager on the other end applies a reverse mapping to get back the original log line
//	// https://github.com/turbot/steampipe/blob/742ae17870f7488e1b610bbaf3ddfa852a58bd3e/cmd/plugin_manager.go#L112
//	//
//	writer := logging.NewEscapeNewlineWriter(os.Stderr)
//
//	// time will be provided by the plugin manager logger
//	logger := logging.NewLogger(&hclog.LoggerOptions{DisableTime: true, Output: writer})
//	log.SetOutput(logger.StandardWriter(&hclog.StandardLoggerOptions{InferLevels: true}))
//	log.SetPrefix("")
//	log.SetFlags(0)
//	return logger
//}
