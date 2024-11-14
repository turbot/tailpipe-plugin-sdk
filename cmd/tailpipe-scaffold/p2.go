package main

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var plugin2Cmd = &cobra.Command{
	Use:   "plugin2",
	Short: "Generates tailpipe plugin/plugin files",
	Run: func(cmd *cobra.Command, args []string) {
		name := viper.GetString("name2")
		location := viper.GetString("location2")

		if name == "" || location == "" {
			fmt.Println("Both 'name' and 'location' must be specified.")
			return
		}
	},
}

func init() {
	// Using Viper to bind flags
	plugin2Cmd.Flags().String("name", "", "Name of the plugin to scaffold")
	plugin2Cmd.Flags().String("location", "", "Location where files should be generated")
	plugin2Cmd.Flags().Bool("source-needed", false, "Flag indicating whether sources files should be created (default: false)")
	viper.BindPFlag("name2", plugin2Cmd.Flags().Lookup("name2"))
	viper.BindPFlag("location2", plugin2Cmd.Flags().Lookup("location2"))
	viper.BindPFlag("source-needed", plugin2Cmd.Flags().Lookup("source-needed"))
}
