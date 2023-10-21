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
	"strings"

	"github.com/spf13/cobra"
)

// grepCmd represents the grep command
var grepCmd = &cobra.Command{
	Use:   "grep",
	Short: "Find messages based on regular expressions.",
	Long: `Function finds messages based on regular expressions. 
		Searches across key, value, message headers
		Supports json,avro,string`,
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

		var messages = KafkaUtils.FindMessage(*config, topic, expression, inputFormat, flags)

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
	},
}

func init() {
	rootCmd.AddCommand(grepCmd)

	grepCmd.Flags().StringVarP(&bootstrap, "bootstrap", "b", "", "Bootstrap Server (bootstrap.server)")
	grepCmd.Flags().StringVarP(&apiKey, "apikey", "a", "", "Optional: APIKey (sasl.username)")
	grepCmd.Flags().StringVarP(&secretKey, "secretkey", "k", "", "Optional: SecretKey (sasl.password)")
	grepCmd.Flags().StringVarP(&topic, "topic", "t", "", "Required: Topic Name")
	grepCmd.Flags().StringVarP(&expression, "expression", "e", "", "Required: Regular expression for match")
	grepCmd.Flags().StringVarP(&output, "output", "o", "", "Type of output (json,table, value[default])")
	grepCmd.Flags().StringVarP(&inputFormat, "input", "i", "", "Input Format (string[default],json,avro,proto[Not Implemented])")
	grepCmd.Flags().StringVarP(&schemaRegistryUrl, "sr-url", "s", "", "Optional: Url of Schema Registry")
	grepCmd.Flags().StringVarP(&schemaRegistryUsername, "sr-user", "u", "", "Optional: Schema Registry Username")
	grepCmd.Flags().StringVarP(&schemaRegistryPassword, "sr-pass", "p", "", "Optional: Schema Registry Password")
	grepCmd.Flags().StringArrayVarP(&xargs, "xarg", "X", make([]string, 0), "Pass optional Configuration Arguments. xargs are passed as -X key=value -X key=value")
}
