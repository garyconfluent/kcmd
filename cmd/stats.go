/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>
*/
package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/viper"
	KafkaUtils "kcmd/kafka"
	"os"
	"regexp"
	"strings"

	"github.com/spf13/cobra"
)

// statsCmd represents the stats command
var statsCmd = &cobra.Command{
	Use:   "stats",
	Short: "A statistical analysis of topics",
	Long: `Provides statistics such as stats,min,max,avg for topics
	`,
	Run: func(cmd *cobra.Command, args []string) {
		viper.AddConfigPath(".")
		viper.SetConfigType("properties")
		viper.SetConfigName("kcmd")

		err := viper.ReadInConfig()
		if err != nil {
			fmt.Printf("Error Loading Configuration %v\n", err)
			os.Exit(1)
		}

		if bootstrap == "" {
			fmt.Println("bootstrap required")
			cmd.Help()
			os.Exit(0)
		}
		//Read Properties from file
		propertyXargs := make(map[string]string)

		//Convert Viper Properties to map
		keys := viper.AllKeys()
		for _, k := range keys {
			propertyXargs[k] = viper.GetString(k)
		}
		for _, k := range xargs {
			xarg := strings.Split(k, "=")
			propertyXargs[xarg[0]] = xarg[1]
		}
		if apiKey != "" {
			propertyXargs["sasl.username"] = apiKey
		}
		if secretKey != "" {
			propertyXargs["sasl.password"] = secretKey
		}
		config, err := KafkaUtils.GetConsumerConfig(bootstrap, propertyXargs)
		aConfig, err := KafkaUtils.GetAdminConfig(bootstrap, propertyXargs)
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
		stats := KafkaUtils.ListTopicsAndStatistics(*config, topics)

		//Output the results
		switch output {
		case "json":
			j, _ := json.MarshalIndent(stats, "", "   ")
			fmt.Print(string(j))
		case "table":
			KafkaUtils.PrintTable(KafkaUtils.TopicStatisticFields, stats, true)
		default:
			for _, c := range stats {
				fmt.Printf("%s %d %.2f %.2f %.2f %.2f\n", c.Topic, c.Count, c.Min, c.Max, c.Avg, c.Median)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(statsCmd)
	statsCmd.Flags().StringVarP(&bootstrap, "bootstrap", "b", "", "Bootstrap Server (bootstrap.server)")
	statsCmd.Flags().StringVarP(&apiKey, "apikey", "a", "", "APIKey (sasl.username)")
	statsCmd.Flags().StringVarP(&secretKey, "secretkey", "s", "", "SecretKey (sasl.password)")
	statsCmd.Flags().StringVarP(&output, "output", "o", "", "Type of output (json,table,key-value)")
	statsCmd.Flags().StringVarP(&filter, "filter", "f", "", "filter topics matching regex")
	statsCmd.Flags().StringArrayVarP(&xargs, "xarg", "X", make([]string, 0), "Pass optional Arguments. xargs are passed as -X key=value -X key=value")
}
