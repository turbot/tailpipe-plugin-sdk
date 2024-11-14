package main

//
//var tableCmd = &cobra.Command{
//	Use:   "table",
//	Short: "Generates tailpipe plugin/table files",
//	Run: func(cmd *cobra.Command, args []string) {
//		name := viper.GetString("name")
//		location := viper.GetString("location")
//		sourceNeeded := viper.GetBool("source-needed")
//
//		if name == "" || location == "" {
//			fmt.Println("Both 'name' and 'location' must be specified.")
//			return
//		}
//
//		generateTableFiles(name, location, sourceNeeded)
//	},
//}
//
//func init() {
//	// Using Viper to bind flags
//	tableCmd.Flags().String("name", "", "Name of the table to scaffold")
//	tableCmd.Flags().String("location", "", "Location where files should be generated")
//	tableCmd.Flags().Bool("source-needed", false, "Flag indicating whether sources files should be created (default: false)")
//	viper.BindPFlag("name", tableCmd.Flags().Lookup("name"))
//	viper.BindPFlag("location", tableCmd.Flags().Lookup("location"))
//	viper.BindPFlag("source-needed", tableCmd.Flags().Lookup("source-needed"))
//}
