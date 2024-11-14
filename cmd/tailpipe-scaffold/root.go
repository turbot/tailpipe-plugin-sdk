package main

import (
	"github.com/spf13/cobra"
	"github.com/turbot/pipe-fittings/cmdconfig"
	"github.com/turbot/pipe-fittings/error_helpers"
	"github.com/turbot/pipe-fittings/utils"
)

var exitCode int

// Build the cobra command that handles our command line tool.
func rootCommand() *cobra.Command {
	// Define our command
	rootCmd := &cobra.Command{
		Use: "tailpipe-scaffold COMMAND [args]",
		Run: func(cmd *cobra.Command, args []string) {
			err := cmd.Help()
			error_helpers.FailOnError(err)
		},
	}

	utils.LogTime("cmd.root.InitCmd start")
	defer utils.LogTime("cmd.root.InitCmd end")

	cmdconfig.
		OnCmd(rootCmd)

	rootCmd.AddCommand(
		pluginCmd(),
		tableCmd(),
	)

	return rootCmd
}

func Execute() int {
	rootCmd := rootCommand()
	utils.LogTime("cmd.root.Execute start")
	defer utils.LogTime("cmd.root.Execute end")

	if err := rootCmd.Execute(); err != nil {
		exitCode = -1
	}
	return exitCode
}
