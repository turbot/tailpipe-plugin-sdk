package main

import "github.com/spf13/cobra"

func init() {
	rootCmd.AddCommand(tableCmd)
	rootCmd.AddCommand(pluginCmd)
}

var rootCmd = &cobra.Command{
	Use:   "tailpipe-scaffold",
	Short: "A tailpipe plugin/table files generator using templates",
}

// Execute adds all child commands to the root command and sets flags appropriately.
func Execute() error {
	return rootCmd.Execute()
}
