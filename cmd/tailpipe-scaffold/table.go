package main

import (
	"fmt"
	"html/template"
	"os"
	"path/filepath"
	"strings"
	"unicode"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var tableCmd = &cobra.Command{
	Use:   "table",
	Short: "Generates tailpipe plugin/table files",
	Run: func(cmd *cobra.Command, args []string) {
		name := viper.GetString("name")
		location := viper.GetString("location")
		sourceNeeded := viper.GetBool("source-needed")

		if name == "" || location == "" {
			fmt.Println("Both 'name' and 'location' must be specified.")
			return
		}

		generateTableFiles(name, location, sourceNeeded)
	},
}

func init() {
	// Using Viper to bind flags
	tableCmd.Flags().String("name", "", "Name of the table to scaffold")
	tableCmd.Flags().String("location", "", "Location where files should be generated")
	tableCmd.Flags().Bool("source-needed", false, "Flag indicating whether sources files should be created (default: false)")
	viper.BindPFlag("name", tableCmd.Flags().Lookup("name"))
	viper.BindPFlag("location", tableCmd.Flags().Lookup("location"))
	viper.BindPFlag("source-needed", tableCmd.Flags().Lookup("source-needed"))
}

// generateTableFiles creates directories and files with content based on the table name, location, and source-needed flag.
func generateTableFiles(name, location string, sourceNeeded bool) {
	directories := []string{"tables", "rows"}
	if sourceNeeded {
		directories = append(directories, "sources")
	}

	// Create directories if they do not exist
	for _, dir := range directories {
		dirPath := filepath.Join(location, dir)
		if _, err := os.Stat(dirPath); os.IsNotExist(err) {
			if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
				fmt.Printf("Error creating directory %s: %v\n", dirPath, err)
				return
			}
		}
	}

	// Define file paths and corresponding templates
	files := map[string]struct {
		Path         string
		PackageName  string
		TemplateFile string
	}{
		"table_config": {
			Path:         filepath.Join(location, "tables", fmt.Sprintf("%s_table_config.go", name)),
			PackageName:  "tables",
			TemplateFile: "templates/table_config.tmpl",
		},
		"table": {
			Path:         filepath.Join(location, "tables", fmt.Sprintf("%s_table.go", name)),
			PackageName:  "tables",
			TemplateFile: "templates/table.tmpl",
		},
		"row": {
			Path:         filepath.Join(location, "rows", fmt.Sprintf("%s.go", name)),
			PackageName:  "rows",
			TemplateFile: "templates/row.tmpl",
		},
	}

	// Add source files if sourceNeeded is true
	if sourceNeeded {
		files["api_source"] = struct {
			Path         string
			PackageName  string
			TemplateFile string
		}{
			Path:         filepath.Join(location, "sources", fmt.Sprintf("%s_api_source.go", name)),
			PackageName:  "sources",
			TemplateFile: "templates/api_source.tmpl",
		}
		files["api_source_config"] = struct {
			Path         string
			PackageName  string
			TemplateFile string
		}{
			Path:         filepath.Join(location, "sources", fmt.Sprintf("%s_api_source_config.go", name)),
			PackageName:  "sources",
			TemplateFile: "templates/api_source_config.tmpl",
		}
	}

	// Create each file with content from templates
	for _, fileData := range files {
		file, err := os.Create(fileData.Path)
		if err != nil {
			fmt.Printf("Error creating file %s: %v\n", fileData.Path, err)
			return
		}
		defer file.Close()

		// Read the template file
		templateContent, err := os.ReadFile(fileData.TemplateFile)
		if err != nil {
			fmt.Printf("Error reading template file %s: %v\n", fileData.TemplateFile, err)
			return
		}

		// Create and parse the template
		tmpl, err := template.New("file").Funcs(template.FuncMap{
			"toPascalCase":          toPascalCase,
			"extractConnectionName": func() string { return extractConnectionName(location) },
		}).Parse(string(templateContent))
		if err != nil {
			fmt.Printf("Error creating template for file %s: %v\n", fileData.Path, err)
			return
		}

		// Execute template
		data := struct {
			PackageName string
			Name        string
		}{
			PackageName: fileData.PackageName,
			Name:        name,
		}
		if err := tmpl.Execute(file, data); err != nil {
			fmt.Printf("Error writing to file %s: %v\n", fileData.Path, err)
			return
		}
	}

	fmt.Printf("Files created successfully for table '%s' in location '%s'\n", name, location)
}

// toPascalCase converts a string to UpperCamelCase (PascalCase).
func toPascalCase(input string) string {
	if input == "" {
		return ""
	}
	words := strings.FieldsFunc(input, func(r rune) bool {
		return r == '_' || r == '-' || unicode.IsSpace(r)
	})
	for i := range words {
		words[i] = strings.Title(strings.ToLower(words[i])) // Capitalize each word, ensuring the rest is lowercase
	}
	return strings.Join(words, "")
}

// extractConnectionName extracts the last segment of the location string (split by '-') and converts it to PascalCase with a 'Connection' suffix.
func extractConnectionName(location string) string {
	// Split the location by '-' to get the last segment
	parts := strings.Split(location, "-")
	if len(parts) == 0 {
		return ""
	}

	// Get the last part and convert to PascalCase
	lastPart := parts[len(parts)-1]
	return toPascalCase(lastPart) + "Connection"
}
