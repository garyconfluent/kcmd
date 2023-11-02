/*
Copyright © 2023 NAME HERE <EMAIL ADDRESS>

*/
package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	KafkaUtils "kcmd/kafka"
	"os"
	"strings"
)

// topicCreateCmd represents the topicCreate command
var topicCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a Topic",
	Long: `Creates a topic`,
	Run: func(cmd *cobra.Command, args []string) {
		if bootstrap == "" || topic == "" {
			fmt.Println("bootstrap, topic required")
			os.Exit(1)
		}
		propertyXArgs := make(map[string]string)
		for _, k := range xargs {
			xarg := strings.Split(k, "=")
			propertyXArgs[xarg[0]] = xarg[1]
		}
		aconfig,err := KafkaUtils.GetAdminConfig(bootstrap,propertyXArgs)
		if err != nil {
			fmt.Printf("Error in Admin Configuration %s \n",err)
		}
		KafkaUtils.CreateTopic(*aconfig,topic,partitions,replicationFactor,timeOut,make(map[string]string))
	},
}

func init() {
	topicCmd.AddCommand(topicCreateCmd)

	topicCreateCmd.Flags().StringVarP(&bootstrap,"bootstrap","b","","Bootstrap Server")
	topicCreateCmd.Flags().StringVarP(&topic,"topic","t","","Topic Name to create")
	topicCreateCmd.Flags().IntVarP(&replicationFactor, "replication-factor","r",1,"Replication Factor. Default:3")
	topicCreateCmd.Flags().IntVarP(&partitions,"partitions","p",3,"# Of Partions to create")
	topicCreateCmd.Flags().StringArrayVarP(&xargs,"xarg","X",make([]string,0),"Pass optional Arguments to Config. xargs are passed as -X key=value -X key=value")
	topicCreateCmd.Flags().StringArrayVarP(&topicArguments,"targ","T", make([]string, 0),"An array of topic arguments pass as -T key=value")
	topicCreateCmd.Flags().IntVar(&timeOut,"timeout",60,"Timeout for Create Topic")
}
