package plugin

import (
	"fmt"
	"github.com/turbot/go-kit/helpers"
	"google.golang.org/grpc"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"time"
)

// ServeOpts are the configurations to serve a plugin.
type ServeOpts struct {
	PluginFunc PluginFunc
}

type PluginFunc func() (TailpipePlugin, error)

const (
	UnrecognizedRemotePluginMessage       = "Unrecognized remote plugin message:"
	UnrecognizedRemotePluginMessageSuffix = "\nThis usually means"
	PluginStartupFailureMessage           = "Plugin startup failed: "
)

// Serve creates and starts the GRPC server which serves the plugin,
//
//	It is called from the main function of the plugin.
//	ServeOpts.PluginFunc must be populated with the plugin constructor function
func Serve(opts *ServeOpts) error {
	if _, isSet := os.LookupEnv("TAILPIPE_DEBUG"); isSet {
		slog.Info("Starting plugin - waiting")
		time.Sleep(10 * time.Second)
		slog.Info("Starting plugin")
	}

	defer func() {
		if r := recover(); r != nil {
			msg := fmt.Sprintf("%s%s", PluginStartupFailureMessage, helpers.ToError(r).Error())
			// write to stdout so the plugin manager can extract the error message
			fmt.Println(msg)
		}
	}()

	s, err := NewPluginServer(opts)
	if err != nil {
		return err
	}
	return s.Serve()
}

func newGRPCServer(options []grpc.ServerOption) *grpc.Server {
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
		slog.Info(fmt.Sprintf("Check http://localhost:%d/debug/pprof/", listener.Addr().(*net.TCPAddr).Port))
		err = http.Serve(listener, nil)
		if err != nil {
			slog.Error("Error starting pprof", "error", err)
		}
	}()
}
