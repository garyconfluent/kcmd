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
	"regexp"
	"strings"
)

// topicListCmd represents the topicList command
var topicListCmd = &cobra.Command{
	Use:   "list",
	Short: "List Topics/Partitions",
	Long:  `List Topics/Partitions`,
	Run: func(cmd *cobra.Command, args []string) {
		if bootstrap == "" {
			fmt.Println("bootstrap required")
			cmd.Help()
			os.Exit(0)
		}

		propertyXargs := make(map[string]string)
		for _, k := range xargs {
			xarg := strings.Split(k, "=")
			propertyXargs[xarg[0]] = xarg[1]
		}

		aConfig, err := KafkaUtils.GetAdminConfig(bootstrap, propertyXargs)
		if err != nil {
			fmt.Printf("Error Getting Admin Config %s\n", err)
			os.Exit(1)
		}
		topics := KafkaUtils.ListTopics(*aConfig)

		if filter != "" {
			tmpTopics := []KafkaUtils.TopicPartition{}
			regFilter, _ := regexp.Compile(filter)
			for _, topic := range topics {
				if regFilter.MatchString(topic.Topic) {
					tmpTopics = append(tmpTopics, topic)
				}
			}
			topics = tmpTopics
		}

		//Output the results
		switch output {
		case "json":
			j, _ := json.MarshalIndent(topics, "", "   ")
			fmt.Print(string(j))
		case "table":
			KafkaUtils.PrintTable(KafkaUtils.TopicFields, topics, true)
		default:
			for _, c := range topics {
				fmt.Printf("%s %d\n", c.Topic, c.Partitions)
			}
		}
	},
}

func init() {
	topicCmd.AddCommand(topicListCmd)

	topicListCmd.Flags().StringVarP(&bootstrap, "bootstrap", "b", "", "Bootstrap Server")
	topicListCmd.Flags().StringArrayVarP(&xargs, "xarg", "X", make([]string, 0), "Pass optional Arguments. xargs are passed as -X key=value -X key=value")
	topicListCmd.Flags().StringVarP(&filter, "filter", "f", "", "filter topics matching regex")
	topicListCmd.Flags().StringVarP(&output, "output", "o", "", "Output Type table,json,key-value[default]")
}
