/*
Copyright © 2023 Gary Vidal <gvidal@confluent.io>
*/
package cmd

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	KafkaUtils "kcmd/kafka"
	"os"
	"strings"
)

// grepCmd represents the find command
var findCmd = &cobra.Command{
	Use:   "find",
	Short: "Finds records in a topic matching a CEL Expression",
	Long: `Finds records in a topic matching a CEL Expression
		Supports ability to search offset, timestamp, key, value and partition
	`,
	Run: func(cmd *cobra.Command, args []string) {

		//Read Properties from file
		if bootstrap == "" || topic == "" || expression == "" {
			fmt.Println("bootstrap,expression,topic required")
			cmd.Help()
			os.Exit(0)
		}
		//Convert Viper Properties to map
		viper.AddConfigPath(".")
		viper.SetConfigType("properties")
		viper.SetConfigName("kcmd")

		err := viper.ReadInConfig()
		if err != nil {
			fmt.Printf("Error Loading Configuration %v\n", err)
			os.Exit(1)
		}
		keys := viper.AllKeys()
		propertyXargs := make(map[string]string)
		for _, k := range keys {
			propertyXargs[k] = viper.GetString(k)
		}

		for _, k := range xargs {
			xarg := strings.Split(k, "=")
			propertyXargs[xarg[0]] = xarg[1]
		}
		//Set Properties from command line
		propertyXargs["bootstrap.servers"] = bootstrap
		if apiKey != "" {
			propertyXargs["sasl.username"] = apiKey
		}
		if secretKey != "" {
			propertyXargs["sasl.password"] = secretKey
		}

		//Flags are custom parameters for schema registry
		flags := make(map[string]string)
		if schemaRegistryUrl != "" {
			flags["schema.registry"] = schemaRegistryUrl
		}
		if schemaRegistryUsername != "" {
			flags["schema.registry.user"] = schemaRegistryUsername
		}
		if schemaRegistryPassword != "" {
			flags["schema.registry.password"] = schemaRegistryPassword
		}

		config, err := KafkaUtils.GetConsumerConfig(bootstrap, propertyXargs)

		var messages = KafkaUtils.FindMessageExpr(*config, topic, expression, inputFormat, flags)

		//Output the results
		switch output {
		case "json":
			j, _ := json.MarshalIndent(messages, "", "   ")
			fmt.Print(string(j))
		case "table":
			KafkaUtils.PrintTable(KafkaUtils.MessageFields, messages, true)
		default:
			for _, c := range messages {
				fmt.Printf("%s", c)
			}
		}
		//fmt.Println("find called")
	},
}
var expression string
var topic string
var schemaRegistryUrl string
var schemaRegistryUsername string
var schemaRegistryPassword string
var inputFormat string
var xargs []string

func init() {
	rootCmd.AddCommand(findCmd)

	findCmd.Flags().StringVarP(&bootstrap, "bootstrap", "b", "", "Bootstrap Server (bootstrap.server)")
	findCmd.Flags().StringVarP(&apiKey, "apikey", "a", "", "APIKey (sasl.username)")
	findCmd.Flags().StringVarP(&secretKey, "secretkey", "k", "", "SecretKey (sasl.password)")
	findCmd.Flags().StringVarP(&topic, "topic", "t", "", "Topic Name")
	findCmd.Flags().StringVarP(&expression, "expression", "e", "", "Regular Expression for match")
	findCmd.Flags().StringVarP(&output, "output", "o", "", "Type of output (json,table,key-value)")
	findCmd.Flags().StringVarP(&inputFormat, "input", "i", "", "Input Format (string[default],json,avro)")
	findCmd.Flags().StringVarP(&schemaRegistryUrl, "sr-url", "s", "", "Url of Schema Registry")
	findCmd.Flags().StringVarP(&schemaRegistryUsername, "sr-user", "u", "", "Schema Registry Username")
	findCmd.Flags().StringVarP(&schemaRegistryPassword, "sr-pass", "p", "", "Schema Registry Password")
	findCmd.Flags().StringArrayVarP(&xargs, "xarg", "X", make([]string, 0), "Pass optional Arguments. xargs are passed as -X key=value -X key=value")
}
