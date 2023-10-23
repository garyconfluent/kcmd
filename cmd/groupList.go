/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	KafkaUtils "kcmd/kafka"
	"os"
	"strings"
)

// groupListCmd represents the groupList command
var groupListCmd = &cobra.Command{
	Use:   "list",
	Short: "List All Consumer Groups",
	Long:  `List all Consumer Groups`,
	Run: func(cmd *cobra.Command, args []string) {

		if bootstrap == "" {
			fmt.Println("bootstrap required")
			cmd.Help()
			os.Exit(1)
		}

		groupConfigArgs := make(map[string]string)
		for _, k := range groupArgs {
			arg := strings.Split(k, "=")
			groupConfigArgs[arg[0]] = arg[1]
		}
		config, err := KafkaUtils.GetAdminConfig(bootstrap, groupConfigArgs)
		if err != nil {
			fmt.Println("Error Creating Kafka Config Map")
		}
		groups := KafkaUtils.ListConsumerGroups(*config, groupFilter)

		//Output the results
		switch output {
		case "json":
			j, _ := json.MarshalIndent(groups, "", "   ")
			fmt.Println(string(j))
		case "table":
			KafkaUtils.PrintTable(KafkaUtils.ConsumerGroupFields, groups, true)
		default:
			for _, g := range groups {
				fmt.Println(g)
			}
		}
	},
}

func init() {
	groupCmd.AddCommand(groupListCmd)
	groupListCmd.Flags().StringVarP(&bootstrap, "bootstrap", "b", "", "Bootstrap Server Url (Required)")
	groupListCmd.Flags().StringArrayVarP(&groupArgs, "args", "X", make([]string, 0), "Configuration Argument key=value")
	groupListCmd.Flags().StringVarP(&groupFilter, "filter", "f", "", "Filter groups by value")
	groupListCmd.Flags().StringVarP(&output, "output", "o", "", "Output Table,Json,Print[default]")
	err := groupListCmd.MarkPersistentFlagRequired("bootstrap")
	if err != nil {
		return
	}
}
