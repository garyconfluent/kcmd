/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"github.com/spf13/cobra"
)

// groupsCmd represents the groups command
var groupCmd = &cobra.Command{
	Use:   "group",
	Short: "Consumer Group Commands",
	Long:  `Consumer Group Commands`,
	Run: func(cmd *cobra.Command, args []string) {
		err := cmd.Help()
		if err != nil {
			return
		}
	},
}
var groupArgs []string
var groupFilter string

func init() {
	rootCmd.AddCommand(groupCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// groupsCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// groupsCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
