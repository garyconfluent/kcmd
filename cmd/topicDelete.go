/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"fmt"
	KafkaUtils "kcmd/kafka"
	"os"
	"strings"

	"github.com/spf13/cobra"
)

// topicDeleteCmd represents the topicDelete command
var topicDeleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Delete a topic",
	Long:  ` Delete a Topic`,
	Run: func(cmd *cobra.Command, args []string) {
		if bootstrap == "" || len(topics) == 0 {
			fmt.Println("bootstrap, topic required")
			os.Exit(1)
		}
		propertyXArgs := make(map[string]string)
		for _, k := range xargs {
			xarg := strings.Split(k, "=")
			propertyXArgs[xarg[0]] = xarg[1]
		}
		aconfig, err := KafkaUtils.GetAdminConfig(bootstrap, propertyXArgs)
		if err != nil {
			fmt.Printf("Error in Admin Configuration %s \n", err)
		}
		KafkaUtils.DeleteTopic(*aconfig, topics, timeOut)
	},
}

func init() {
	topicCmd.AddCommand(topicDeleteCmd)

	topicDeleteCmd.Flags().StringVarP(&bootstrap, "bootstrap", "b", "", "Bootstrap Server")
	topicDeleteCmd.Flags().StringArrayVarP(&topics, "topic", "t", make([]string, 0), "Topic Name(s) to delete")
	topicDeleteCmd.Flags().StringArrayVarP(&xargs, "xarg", "X", make([]string, 0), "Pass optional Arguments to Config. xargs are passed as -X key=value -X key=value")
}
