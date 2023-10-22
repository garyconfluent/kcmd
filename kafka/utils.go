package KafkaUtils

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"github.com/google/cel-go/cel"
	"github.com/google/cel-go/ext"
	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/montanaflynn/stats"
	"github.com/riferrei/srclient"
	"github.com/xyproto/randomstring"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

var TopicCountFields = []interface{}{"Topic", "Count"}
var MessageFields = []interface{}{"Key", "Value", "Timestamp", "Partition", "Offset"}
var TopicStatisticFields = []interface{}{"Topic", "Count", "Min", "Max", "Median"}
var celEnvOptions = []cel.EnvOption{
	cel.EagerlyValidateDeclarations(true),
	cel.DefaultUTCTimeZone(true),
	ext.Strings(ext.StringsVersion(2)),
	cel.CrossTypeNumericComparisons(true),
	cel.OptionalTypes(), /*,
	k8s.URLs(),
	k8s.Regex(),
	k8s.Lists(),
	k8s.Quantity(),*/
}

type TopicCount struct {
	Topic string `json:"topic"`
	Count int    `json:"count"`
}
type TopicStatistic struct {
	Topic  string  `json:"topic"`
	Count  int     `json:"count"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
	Avg    float64 `json:"avg"`
	Median float64 `json:"median"`
}
type TopicPartition struct {
	Topic      string `json:"topic"`
	Partitions int    `json:"partitions"`
}

type MessageInfo struct {
	Key       string            `json:"key"`
	Value     any               `json:"value"`
	Timestamp time.Time         `json:"timestamp"`
	Partition int32             `json:"partition"`
	Offset    int32             `json:"offset"`
	Headers   map[string]string `json:"headers"`
}
type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

/*
 * Returns the AdminConfiguration required for ListTopics
 */
func GetAdminConfig(bootstrap string, propertyArgs map[string]string) (*kafka.ConfigMap, error) {
	config := &kafka.ConfigMap{}
	if err := config.SetKey("bootstrap.servers", bootstrap); err != nil {
		return nil, err
	}
	//Iterate propartyArges and set value in ConfigMap
	if len(propertyArgs) > 0 {
		for key, value := range propertyArgs {
			if err := config.SetKey(key, value); err != nil {
				return nil, err
			}
		}
	}
	return config, nil
}

/*
 * Return the COnsumer Config and then optionally add custom parameters
 */
func GetConsumerConfig(bootstrap string, propertyArgs map[string]string) (*kafka.ConfigMap, error) {
	config := &kafka.ConfigMap{}
	if err := config.SetKey("bootstrap.servers", bootstrap); err != nil {
		return nil, err
	}
	if err := config.SetKey("client.id", "kafkautils.count"); err != nil {
		return nil, err
	}
	if err := config.SetKey("group.id", "kafkautils-"+randomstring.EnglishFrequencyString(4)); err != nil {
		return nil, err
	}
	if err := config.SetKey("auto.offset.reset", "beginning"); err != nil {
		return nil, err
	}
	if err := config.SetKey("enable.partition.eof", true); err != nil {
		return nil, err
	}
	// see explanation: https://www.confluent.io/blog/incremental-cooperative-rebalancing-in-kafka/
	if err := config.SetKey("partition.assignment.strategy", "cooperative-sticky"); err != nil {
		return nil, err
	}
	if len(propertyArgs) > 0 {
		for key, value := range propertyArgs {
			if err := config.SetKey(key, value); err != nil {
				return nil, err
			}
		}
	}
	return config, nil
}

/*
* List of Topics and Partitions
 */
func ListTopics(config kafka.ConfigMap) []TopicPartition {
	ac, err := kafka.NewAdminClient(&config)
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}
	lm, lmerr := ac.GetMetadata(nil, true, 100000)
	if lmerr != nil {
		fmt.Printf("Failed to get Topic Metadata: %s\n", lmerr)
		os.Exit(1)
	}
	var topicNames = []TopicPartition{}
	for _, v := range lm.Topics {
		topicNames = append(topicNames, TopicPartition{v.Topic, len(v.Partitions)})
	}
	ac.Close()
	return topicNames
}

/*
 *  List of topics and Count number of messages.
 */
func ListTopicsAndCount(config kafka.ConfigMap, topics []TopicPartition, estimate bool) []TopicCount {
	topicCounts := []TopicCount{}
	var wg sync.WaitGroup
	for i := 0; i < len(topics); i++ {
		wg.Add(1)
		topic := topics[i]
		go func(_topic TopicPartition) {
			var count TopicCount
			if estimate {
				count = FastCountMessages(config, _topic.Topic, _topic.Partitions)
			} else {
				count = CountMessages(config, _topic.Topic, _topic.Partitions)
			}
			defer wg.Done()
			topicCounts = append(topicCounts, count)
		}(topic)
	}
	wg.Wait()

	//Sort Array
	sort.SliceStable(topicCounts, func(i, j int) bool {
		return topicCounts[i].Topic < topicCounts[j].Topic
	})

	return topicCounts
}

/*
 *  List of topics and Output Statitics
 */
func ListTopicsAndStatistics(config kafka.ConfigMap, topics []TopicPartition) []TopicStatistic {
	var topicStats []TopicStatistic
	var wg sync.WaitGroup
	for i := 0; i < len(topics); i++ {
		wg.Add(1)
		topic := topics[i]
		go func(_topic TopicPartition) {
			var statistics = StatMessage(config, _topic.Topic, _topic.Partitions)
			defer wg.Done()
			topicStats = append(topicStats, statistics)
		}(topic)
	}
	wg.Wait()

	//Sort Array
	sort.SliceStable(topicStats, func(i, j int) bool {
		return topicStats[i].Topic < topicStats[j].Topic
	})

	return topicStats
}

/*
 * Create a Consumer and executes count
 */
func CountMessages(config kafka.ConfigMap, topic string, partitions int) TopicCount {
	//fmt.Printf("Counting Topic:%s", topic)
	run := true
	ctr := 0
	partCount := partitions
	var consumer, err = kafka.NewConsumer(&config)
	if err != nil {
		fmt.Println("Failed to create consumer %s\n", err)
		run = false
	}
	serr := consumer.Subscribe(topic, nil)
	if serr != nil {
		fmt.Println("Failed to subscribe topic: %s\n", serr)
		run = false
	}

	for run == true {
		ev := consumer.Poll(10000)
		switch ev.(type) {
		case *kafka.Message:
			ctr++
		case kafka.Error:
			fmt.Printf("Error in Consumer Count %s\n", ev)
			run = false
			partCount = 0
			ctr = -1
		case kafka.PartitionEOF:
			partCount--
			if partCount == 0 {
				run = false
			}
		}
	}
	err = consumer.Close()
	if err != nil {
		fmt.Println(err)
	}
	return TopicCount{Topic: topic, Count: ctr}
}

/*
 * Create a Consumer and executes count
 */
func StatMessage(config kafka.ConfigMap, topic string, partitions int) TopicStatistic {
	//fmt.Printf("Counting Topic:%s", topic)
	run := true
	ctr := 0
	lenArray := []float64{}
	partCount := partitions
	var consumer, err = kafka.NewConsumer(&config)
	if err != nil {
		fmt.Println("Failed to create consumer %s\n", err)
		run = false
	}
	serr := consumer.Subscribe(topic, nil)
	if serr != nil {
		fmt.Println("Failed to subscribe topic: %s\n", serr)
		run = false
	}

	for run == true {
		ev := consumer.Poll(10000)
		switch m := ev.(type) {
		case *kafka.Message:
			ctr++
			length := len(m.Value)
			lenArray = append(lenArray, float64(length))
		case kafka.Error:
			fmt.Printf("Error in Consumer Count %s\n", ev)
			run = false
			partCount = 0
			ctr = -1
		case kafka.PartitionEOF:
			partCount--
			if partCount == 0 {
				run = false
			}
		}
	}
	err = consumer.Close()
	if err != nil {
		fmt.Println(err)
	}
	minVal, _ := stats.Min(lenArray)
	maxVal, _ := stats.Max(lenArray)
	median, _ := stats.Median(lenArray)
	if ctr == 0 {
		return TopicStatistic{Topic: topic, Count: ctr}
	}
	return TopicStatistic{
		Topic:  topic,
		Count:  ctr,
		Min:    minVal,
		Max:    maxVal,
		Avg:    average(lenArray),
		Median: median}
}
func average(xs []float64) float64 {
	total := 0.0
	for _, v := range xs {
		total += v
	}
	return total / float64(len(xs))
}

/*
 * Function does a fast count by using offsets and watermarks
 */
func FastCountMessages(config kafka.ConfigMap, topic string, partitions int) TopicCount {

	var consumer, err = kafka.NewConsumer(&config)

	err = consumer.Subscribe(topic, nil)
	if err != nil {
		fmt.Println("Failed to subscribe topic: %s\n", err)
	}
	ac, acerr := kafka.NewAdminClientFromConsumer(consumer)
	if acerr != nil {
		fmt.Println("Error getting Consumer Admin Client")
		os.Exit(1)
	}

	metadata, _ := ac.GetMetadata(&topic, false, 10000)
	parts := []kafka.PartitionMetadata{}
	for k, _ := range metadata.Topics {
		parts = metadata.Topics[k].Partitions
	}
	count := int64(0)
	for _, part := range parts {
		low, high, err := consumer.QueryWatermarkOffsets(topic, part.ID, 10000)
		if err != nil {
			return TopicCount{topic, -1}
		}
		count += high - low //fmt.Printf("Topic %s,Partition: %d Offsets: %d %d\n", topic, part.ID, low, high)
	}
	time.Sleep(2 * time.Second) //Thread Panic here let sleep before connection is closed
	ac.Close()
	_ = consumer.Close()

	return TopicCount{topic, int(count)}
}

/*
* Grep messages based on a regex pattern, optionally deserializes output based on schema registry
 */
func GrepMessage(config kafka.ConfigMap, topic string, reg string, input string, flags map[string]string) []MessageInfo {
	//Compile regex
	regexFind, rerr := regexp.Compile(reg)
	if rerr != nil {
		fmt.Printf("Failed to compile regex %s\n", rerr)
		os.Exit(1)
	}

	var consumer, err = kafka.NewConsumer(&config)
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		fmt.Println("Failed to subscribe topic: %s\n", err)
		os.Exit(1)
	}
	var ac, acerr = kafka.NewAdminClientFromConsumer(consumer)
	if acerr != nil {
		fmt.Println("Failed to get Admin Client: %s\n", acerr)
		os.Exit(1)
	}
	var tMetadata, merr = ac.GetMetadata(&topic, false, 10000)
	if merr != nil {
		fmt.Println("Failed to get Metadata: %s\n", merr)
		os.Exit(1)
	}
	var partCount = 1
	for _, v := range tMetadata.Topics {
		partCount = len(v.Partitions)
		break
	}

	//Initialize Schema Registry even if its not used.
	//Dont know how to post assign client
	sru := flags["schema.registry"]
	sruser := flags["schema.registry.user"]
	srpass := flags["schema.registry.password"]
	srClient := srclient.CreateSchemaRegistryClient(sru)
	if sruser != "" && srpass != "" {
		srClient.SetCredentials(sruser, srpass)
	}
	srClient.CachingEnabled(true)
	srClient.SetTimeout(time.Minute)

	run := true
	messages := make([]MessageInfo, 0)
	for run == true {
		ev := consumer.Poll(10000)
		switch e := ev.(type) {
		case *kafka.Message:
			msgValue := string(e.Value)
			msgKey := string(e.Key[:])
			msgHeaders := e.Headers

			switch input {
			case "json":
				err := json.Unmarshal(e.Value, &msgValue)
				if err != nil {
					fmt.Printf("Error unmarshalling json %s\n", err)
				}
			case "proto":
				fmt.Println("Cannot support protobuf at this time")
				os.Exit(1)
			case "avro":
				schemaId := binary.BigEndian.Uint32(e.Value[1:5])
				schema, err := srClient.GetSchema(int(schemaId))
				if err != nil {
					panic(fmt.Sprintf("Error getting the schema with id '%d' %s", schemaId, err))
				}
				native, _, _ := schema.Codec().NativeFromBinary(e.Value[5:])
				value, _ := schema.Codec().TextualFromNative(nil, native)
				msgValue = string(value)
			default:
				msgValue = string(e.Value[:])
			}

			isMatchValue := regexFind.MatchString(msgValue)
			isMatchKey := regexFind.MatchString(msgKey)
			isMatchHeaders := false
			for _, v := range msgHeaders {
				if regexFind.MatchString(v.Key) || regexFind.MatchString(string(v.Value[:])) {
					isMatchHeaders = true
					break
				}
			}

			if isMatchValue || isMatchKey || isMatchHeaders {
				outputHeaders := make(map[string]string)
				for _, v := range msgHeaders {
					outputHeaders[v.Key] = string(v.Value[:])
				}
				msgInfo := MessageInfo{
					string(msgKey),
					string(msgValue),
					e.Timestamp,
					e.TopicPartition.Partition,
					int32(e.TopicPartition.Offset),
					outputHeaders}
				messages = append(messages, msgInfo)
			}
		case kafka.Error:
			fmt.Printf("Error in Consumer Find %s\n", ev)
			run = false
			partCount = 0
		case kafka.PartitionEOF:
			partCount--
			if partCount == 0 {
				run = false
			}
		}
	}
	err = consumer.Close()
	if err != nil {
		fmt.Println(err)
	}
	return messages
}

/*
* Finds messages based on a CEL Expresssion, optionally deserializes output based on schema registry
 */
func FindMessageExpr(config kafka.ConfigMap, topic string, expr string, input string, flags map[string]string) []MessageInfo {
	//Compile expression

	var consumer, err = kafka.NewConsumer(&config)
	err = consumer.Subscribe(topic, nil)
	if err != nil {
		fmt.Println("Failed to subscribe topic: %s\n", err)
		//run = false
		os.Exit(1)
	}
	var ac, acerr = kafka.NewAdminClientFromConsumer(consumer)
	if acerr != nil {
		fmt.Println("Failed to get Admin Client: %s\n", acerr)
		os.Exit(1)
	}
	var tMetadata, merr = ac.GetMetadata(&topic, false, 10000)
	if merr != nil {
		fmt.Println("Failed to get Metadata: %s\n", merr)
		os.Exit(1)
	}
	var partCount = 1
	for _, v := range tMetadata.Topics {
		partCount = len(v.Partitions)
		break
	}

	//Initialize Schema Registry even if its not used.
	//Dont know how to post assign client
	sru := flags["schema.registry"]
	sruser := flags["schema.registry.user"]
	srpass := flags["schema.registry.password"]
	//srclient create Schema Registry
	srClient := srclient.CreateSchemaRegistryClient(sru)
	if sruser != "" && srpass != "" {
		srClient.SetCredentials(sruser, srpass)
	}
	srClient.CachingEnabled(true)
	srClient.SetTimeout(time.Minute)

	//Compile Expression and build structures to evaluate
	env, _ := cel.NewEnv(
		cel.Variable("key", cel.StringType),
		cel.Variable("value", cel.AnyType),
		cel.Variable("timestamp", cel.TimestampType),
		cel.Variable("offset", cel.IntType),
		cel.Variable("headers", cel.AnyType),
		cel.Variable("partition", cel.IntType),
	)
	ast, iss := env.Compile(expr)
	if iss.Err() != nil {
		fmt.Println(iss.Err())
		os.Exit(1)
	}
	prg, err := env.Program(ast)
	if err != nil {
		fmt.Println(iss.Err())
		os.Exit(1)
	}

	messages := make([]MessageInfo, 0)
	run := true
	for run == true {
		ev := consumer.Poll(10000)
		switch e := ev.(type) {
		case *kafka.Message:
			var msgValue any
			msgKey := string(e.Key[:])
			msgHeaders := e.Headers
			outputHeaders := make(map[string]string, 0)
			for _, v := range msgHeaders {
				outputHeaders[v.Key] = string(v.Value[:])
			}
			switch input {
			case "json":
				_ = json.Unmarshal(e.Value, &msgValue)
			case "proto":
				fmt.Println("Cannot support protobuf at this time")
				os.Exit(1)

			case "avro":
				schemaId := binary.BigEndian.Uint32(e.Value[1:5])
				schema, err := srClient.GetSchema(int(schemaId))
				if err != nil {
					panic(fmt.Sprintf("Error getting the schema with id '%d' %s", schemaId, err))
				}
				native, _, _ := schema.Codec().NativeFromBinary(e.Value[5:])
				value, _ := schema.Codec().TextualFromNative(nil, native)
				_ = json.Unmarshal(value, &msgValue)
			default:
				msgValue = string(e.Value[:])
			}
			out, _, err := prg.Eval(map[string]any{
				"key":       msgKey,
				"value":     msgValue,
				"timestamp": e.Timestamp,
				"offset":    e.TopicPartition.Offset,
				"partition": e.TopicPartition.Partition,
				"headers":   outputHeaders,
			})
			if err != nil {
				fmt.Printf("Evaluation Error: %s\n", err)
			}
			isMatch, _ := strconv.ParseBool(fmt.Sprint(out))
			if isMatch {
				msgInfo := MessageInfo{
					string(msgKey),
					msgValue,
					e.Timestamp,
					e.TopicPartition.Partition,
					int32(e.TopicPartition.Offset),
					outputHeaders}
				messages = append(messages, msgInfo)
			}
		case kafka.Error:
			fmt.Printf("Error in Consumer Find %s\n", ev)
			run = false
			partCount = 0
		case kafka.PartitionEOF:
			partCount--
			if partCount == 0 {
				run = false
			}
		}
	}
	err = consumer.Close()
	if err != nil {
		fmt.Println(err)
	}
	return messages
}

/*
* Prints a struct to a table
 */
func PrintTable(fields []interface{}, objects interface{}, includeOrder bool) {
	if includeOrder {
		fields = append([]interface{}{""}, fields...)
	}
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(fields)

	for idx, object := range ConvertToInterfaceArray(objects) {
		var elements []interface{}
		if includeOrder {
			elements = append(elements, idx)
		}
		r := reflect.ValueOf(object)
		for i, field := range fields {
			if i == 0 && includeOrder {
				continue
			}
			f := reflect.Indirect(r).FieldByName(field.(string))
			elements = append(elements, f)
		}
		t.AppendRow(elements)
	}
	t.AppendSeparator()
	t.Render()
}

/*
* Converts an interface to String array of interfaces
 */
func ConvertToInterfaceArray(object interface{}) []interface{} {
	o := reflect.ValueOf(object)
	slice := make([]interface{}, o.Len())
	for i := 0; i < o.Len(); i++ {
		slice[i] = o.Index(i).Interface()
	}
	return slice
}
