package main

//
//import (
//	"fmt"
//	"os"
//	"path/filepath"
//	"text/template"
//
//	"github.com/spf13/cobra"
//	"github.com/spf13/viper"
//)
//
//func pluginCmd_()*cobra.Command {
//	cmd := &cobra.Command{
//		Use:   "plugin",
//		Short: "Generates a plugin folder and a plugin.go file",
//		Run: func(cmd *cobra.Command, args []string) {
//			name := viper.GetString("name")
//			location := viper.GetString("location")
//
//			fmt.Println(name, location)
//
//			if name == "" || location == "" {
//				fmt.Println("Both 'name' and 'location' must be specified.")
//				return
//			}
//
//			generatePluginFiles(name, location)
//		},
//	}
//	// Using Viper to bind flags
//	cmd.Flags().String("name", "", "Name of the plugin to scaffold")
//	cmd.Flags().String("location", "", "Location where files should be generated")
//	viper.BindPFlag("name", cmd.Flags().Lookup("name"))
//	viper.BindPFlag("location", cmd.Flags().Lookup("location"))
//
//	return cmd
//}
//
//// generatePluginFiles creates a folder named after the plugin and generates a plugin.go file.
//func generatePluginFiles(name, location string) {
//	// Create the plugin directory
//	pluginDir := filepath.Join(location, name)
//	if _, err := os.Stat(pluginDir); os.IsNotExist(err) {
//		if err := os.MkdirAll(pluginDir, os.ModePerm); err != nil {
//			fmt.Printf("Error creating directory %s: %v\n", pluginDir, err)
//			return
//		}
//	}
//
//	// Path for the plugin.go file
//	pluginFilePath := filepath.Join(pluginDir, "plugin.go")
//
//	// Template for the plugin.go file
//	const pluginTemplate = `package {{ .PackageName }}
//
//import "fmt"
//
//// {{ .PackageName }}Plugin is a sample plugin struct.
//type {{ .PackageName }}Plugin struct{}
//
//// Initialize initializes the plugin.
//func (p *{{ .PackageName }}Plugin) Initialize() {
//	fmt.Println("Initializing {{ .PackageName }} plugin")
//}
//`
//
//	// Create the plugin.go file
//	file, err := os.Create(pluginFilePath)
//	if err != nil {
//		fmt.Printf("Error creating file %s: %v\n", pluginFilePath, err)
//		return
//	}
//	defer file.Close()
//
//	// Create and execute the template
//	tmpl, err := template.New("plugin").Parse(pluginTemplate)
//	if err != nil {
//		fmt.Printf("Error creating template: %v\n", err)
//		return
//	}
//
//	data := struct {
//		PackageName string
//	}{
//		PackageName: name,
//	}
//
//	if err := tmpl.Execute(file, data); err != nil {
//		fmt.Printf("Error writing to file %s: %v\n", pluginFilePath, err)
//		return
//	}
//
//	fmt.Printf("Plugin files created successfully for '%s' in location '%s'\n", name, location)
//}
