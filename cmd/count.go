/*
Copyright © 2023 Gary Vidal <gvidal@confluent.io>
*/
package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"kcmd/kafka"
	"os"
	"regexp"
	"strings"
)

var bootstrap string
var apiKey string
var secretKey string
var output string
var filter string
var estimate bool

// countCmd represents the count command
var countCmd = &cobra.Command{
	Use:   "count",
	Short: "Counts all topics records",
	Long:  `Counts all topic records`,
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
		counts := KafkaUtils.ListTopicsAndCount(*config, topics, estimate)

		//Output the results
		switch output {
		case "json":
			j, _ := json.MarshalIndent(counts, "", "   ")
			fmt.Print(string(j))
		case "table":
			KafkaUtils.PrintTable(KafkaUtils.TopicCountFields, counts, true)
		default:
			for _, c := range counts {
				fmt.Printf("%s %d\n", c.Topic, c.Count)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(countCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// countCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	countCmd.Flags().StringVarP(&bootstrap, "bootstrap", "b", "", "Bootstrap Server (bootstrap.server)")
	countCmd.Flags().StringVarP(&apiKey, "apikey", "a", "", "APIKey (sasl.username)")
	countCmd.Flags().StringVarP(&secretKey, "secretkey", "s", "", "SecretKey (sasl.password)")
	countCmd.Flags().StringVarP(&output, "output", "o", "", "Type of output (json,table,key-value)")
	countCmd.Flags().StringVarP(&filter, "filter", "f", "", "filter topics matching regex")
	countCmd.Flags().StringArrayVarP(&xargs, "xarg", "X", make([]string, 0), "Pass optional Arguments. xargs are passed as -X key=value -X key=value")
	countCmd.Flags().BoolVarP(&estimate, "estimate", "e", true, "Estimate the output using watermarks[true] else counts[false]")
}
