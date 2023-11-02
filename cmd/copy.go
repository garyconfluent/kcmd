/*
Copyright © 2023 Gary Vidal <gvidal@confluent.io
*/
package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	KafkaUtils "kcmd/kafka"
	"os"
	"strings"
	"time"
)

// copyCmd represents the copy command
var copyCmd = &cobra.Command{
	Use:   "copy",
	Short: "Copy messages from one topic to another",
	Long: `Copy messages from one broker/topic to another broker/topic.
	Optionally filter messages before copying supports Avro,JSON,string

`,
	Run: func(cmd *cobra.Command, args []string) {

		//Populate Input Config Arguments
		inputConfigs := make(map[string]string)
		for _, k := range inputArgs {
			arg := strings.Split(k, "=")
			inputConfigs[arg[0]] = arg[1]
		}

		//Populate Output(producer) Config Arguments
		outputConfigs := make(map[string]string)
		for _, z := range outputArgs {
			arg := strings.Split(z, "=")
			var key = arg[0]
			var value = arg[1]
			outputConfigs[key] = value
		}

		consumerConfigMap, ccmerr := KafkaUtils.GetConsumerConfig(inputBootstrap, inputConfigs)
		if ccmerr != nil {
			fmt.Printf("Consumer Config Error %s\n", ccmerr)
			os.Exit(1)
		}
		producerConfigMap, pcmerr := KafkaUtils.GetProducerConfig(outputBootstrap, outputConfigs)
		if pcmerr != nil {
			fmt.Printf("Producer Config Error %s\n", ccmerr)
			os.Exit(1)
		}

		//Flags are custom parameters for schema registry
		flags := make(map[string]string)
		if copySchemaRegistryUrl != "" {
			flags["schema.registry"] = copySchemaRegistryUrl
		}
		if copySchemaRegistryUser != "" {
			flags["schema.registry.user"] = copySchemaRegistryUser
		}
		if copySchemaRegistryPass != "" {
			flags["schema.registry.password"] = copySchemaRegistryPass
		}
		if copyFilter != "" {
			flags["copyFilter"] = copyFilter
		}
		if copyFormat != "" {
			flags["copyFormat"] = copyFormat
		}
		if offsetTimestamp != "" {
			var offsetTime, err = time.Parse(time.RFC3339, offsetTimestamp)
			if err != nil {
				fmt.Printf("Error Formatting Time%s\n", err)
				os.Exit(1)

			}
			KafkaUtils.CopyMessages(*consumerConfigMap, *producerConfigMap, inputTopic, outputTopic, continuous, offsetTime, flags)
		} else {
			KafkaUtils.CopyMessages(*consumerConfigMap, *producerConfigMap, inputTopic, outputTopic, continuous, time.Time{}, flags)
		}

	},
}

func init() {
	rootCmd.AddCommand(copyCmd)

	// Here you will define your flags and configuration settings.
	copyCmd.Flags().StringVar(&inputBootstrap, "input-bs", "", "Input Bootstrap Url")
	copyCmd.Flags().StringVar(&outputBootstrap, "output-bs", "", "Output Bootstrap Url")
	copyCmd.Flags().StringVar(&inputTopic, "input-topic", "", "Input Topic Name")
	copyCmd.Flags().StringVar(&outputTopic, "output-topic", "", "Output Topic Name")

	copyCmd.Flags().BoolVarP(&continuous, "continuous", "c", false, "Run Command Continuously")

	copyCmd.Flags().StringVar(&consumerConfig, "c-config", "", "Path To Consumer Config")
	copyCmd.Flags().StringVar(&producerConfig, "p-config", "", "Path To Producer Config")

	copyCmd.Flags().StringVar(&copyFormat, "input-format", "", "Input format(json,avro,string) - Used to deserialize messages for expression")
	copyCmd.Flags().StringVar(&copyFilter, "filter", "", "Filter applied to input topic to filter messages")

	copyCmd.Flags().StringVar(&copySchemaRegistryUrl, "sr-url", "", "Schema Registry Url")
	copyCmd.Flags().StringVar(&copySchemaRegistryUser, "sr-user", "", "Schema Registry User")
	copyCmd.Flags().StringVar(&copySchemaRegistryPass, "sr-pass", "", "Schema Registry Password")

	copyCmd.Flags().StringVar(&offsetTimestamp, "offset-timestamp", "", "Offset Timestamp to start in RFC3339 format YYYY-MM-DDTHH:mm:SSZ")

	copyCmd.Flags().StringArrayVarP(&inputArgs, "iarg", "I", make([]string, 0), "Pass optional Arguments. iargs are passed as -I key=value -X key=value")
	copyCmd.Flags().StringArrayVarP(&outputArgs, "oarg", "O", make([]string, 0), "Pass optional Arguments. oargs are passed as -O key=value -X key=value")
}
