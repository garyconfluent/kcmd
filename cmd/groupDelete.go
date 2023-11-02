/*
Copyright © 2023 Gary Vidal <gvidal@confluent.io>
*/
package cmd

import (
	"fmt"
	KafkaUtils "kcmd/kafka"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// groupDeleteCmd represents the groupDelete command
var groupDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete Consumer Group(s)",
	Long:  `Delete Consumer Group(s)`,
	Run: func(cmd *cobra.Command, args []string) {

		if bootstrap == "" || len(groups) == 0 {
			fmt.Println("bootstrap and group are required")
			cmd.Help()
			os.Exit(1)
		}

		groupConfigArgs := make(map[string]string)
		for _, k := range groupArgs {
			xarg := strings.Split(k, "=")
			groupConfigArgs[xarg[0]] = xarg[1]
		}

		config, err := KafkaUtils.GetAdminConfig(bootstrap, groupConfigArgs)
		if err != nil {
			fmt.Println("Error Creating Kafka Config Map")
		}

		KafkaUtils.DeleteConsumerGroup(*config, groups)
	},
}

func init() {
	groupCmd.AddCommand(groupDeleteCmd)
	groupDeleteCmd.Flags().StringArrayVarP(&groups, "group", "g", make([]string, 0), "Group Flag for each group to delete")
	groupDeleteCmd.Flags().StringVarP(&bootstrap, "bootstrap", "b", "", "Bootstrap Server Url")
	groupDeleteCmd.Flags().StringArrayVarP(&groupArgs, "args", "X", make([]string, 0), "Configuration Argument key=value")
	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// groupDeleteCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// groupDeleteCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
}
