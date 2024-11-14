package main

import (
	"fmt"
	"os"
	"path/filepath"
	"text/template"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/turbot/pipe-fittings/cmdconfig"
)

func pluginCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use: "plugin [flags]",
		Run: runPluginCmd,
	}

	cwd, _ := os.Getwd()
	cmdconfig.OnCmd(cmd).
		AddStringFlag("location", cwd, "Specify the location to create a plugin folder").
		AddStringFlag("name", "", "Specify the plugin name")

	return cmd
}

func runPluginCmd(cmd *cobra.Command, args []string) {
	name := viper.GetString("name")
	location := viper.GetString("location")

	if name == "" || location == "" {
		fmt.Println("Both 'name' and 'location' must be specified.")
		return
	}

	generatePluginFiles(name, location)
}

// generatePluginFiles creates a folder named after the plugin and generates a plugin.go file.
func generatePluginFiles(name, location string) {
	// Create the plugin directory
	pluginDir := filepath.Join(location, name)
	if _, err := os.Stat(pluginDir); os.IsNotExist(err) {
		if err := os.MkdirAll(pluginDir, os.ModePerm); err != nil {
			fmt.Printf("Error creating directory %s: %v\n", pluginDir, err)
			return
		}
	}

	// Path for the plugin.go file
	pluginFilePath := filepath.Join(pluginDir, "plugin.go")

	// Template for the plugin.go file
	const pluginTemplate = `package {{ .PackageName }}

import (
	"github.com/turbot/go-kit/helpers"
	"github.com/turbot/tailpipe-plugin-{{ .PackageName }}/config"
	"github.com/turbot/tailpipe-plugin-sdk/plugin"
	// reference the table package to ensure that the tables are registered by the init functions
	_ "github.com/turbot/tailpipe-plugin-{{ .PackageName }}/tables"
)

type Plugin struct {
	plugin.PluginImpl
}

func NewPlugin() (_ plugin.TailpipePlugin, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = helpers.ToError(r)
		}
	}()

	p := &Plugin{
		PluginImpl: plugin.NewPluginImpl("{{ .PackageName }}", config.New{{ .PackageName | toPascalcase }}Connection),
	}

	return p, nil
}
`

	// Create the plugin.go file
	file, err := os.Create(pluginFilePath)
	if err != nil {
		fmt.Printf("Error creating file %s: %v\n", pluginFilePath, err)
		return
	}
	defer file.Close()

	// Create and execute the template
	tmpl, err := template.New("plugin").
		Funcs(template.FuncMap{
			"toPascalcase": toPascalCase,
		}).Parse(pluginTemplate)
	if err != nil {
		fmt.Printf("Error creating template: %v\n", err)
		return
	}

	data := struct {
		PackageName string
	}{
		PackageName: name,
	}

	if err := tmpl.Execute(file, data); err != nil {
		fmt.Printf("Error writing to file %s: %v\n", pluginFilePath, err)
		return
	}

	fmt.Printf("Plugin files created successfully for '%s' in location '%s'\n", name, location)
}
