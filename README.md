# KCMD Readme
 
Command Line Utilities for Kafka written in Go,
Provides common tools to analyze, display, find and copy messages in Kafka.


## Commands

### `find` 

Finds records in a topic using CEL Expression Language
(https://github.com/google/cel-spec)
For a list of builtin functions 
(https://www.krakend.io/docs/enterprise/security-policies/built-in-functions/). 
Optionally can transform message from avro,json,protobuf and connect to schema-registry

#### Args

```bash
Finds records in a topic matching a CEL Expression
including values in Key, Value, Offset, Timestamp and Headers

Searches key, value and header values

Usage:
  kcmd find [flags]

Flags:
  -a, --apikey string       APIKey (sasl.username)
  -b, --bootstrap string    Bootstrap Server (bootstrap.server)
  -e, --expression string   Regular Expression for match
  -h, --help                help for find
  -i, --input string        Input Format (string,json,avro)
  -o, --output string       Type of output (json,table,key-value)
  -k, --secretkey string    SecretKey (sasl.password)
  -p, --sr-pass string      Schema Registry Password
  -s, --sr-url string       Url of Schema Registry
  -u, --sr-user string      Schema Registry Username
  -t, --topic string        Topic Name
```

#### Examples
```bash
#Find Topics matching a value input of string
./kcmd find -b <bootstrap-url> \
              -t datagen-topic \
              -e "offset > 10000 && offset < 10010" \ 
              -o json

#Find value of message using avro format and schema registry
# All inputs of avro are treated as strings just parsed to json 
./kcmd find -b <bootstrap-url> \
              -a <apiKey> \
              -k <secretKey> \
              -t datagen-topic \ 
              -e "offset > 1000 && offset < 1010 " \
              -o json \
              -i avro \ 
              -s <schema-registry-url> \
              -u <schema-registry-apikey> \ 
              -p <schema-registry-secretKey>\
              -X sasl.mechanism=PLAIN \
              -X security.protocol=SASL_SSL
```
#### Example with Complex Expression
```json
 {
    "key": "ZVZZT",
    "value": {
      "account": "ABC123",
      "price": 976,
      "quantity": 3881,
      "side": "BUY",
      "symbol": "ZVZZT",
      "userid": "User_6"
    },
    "timestamp": "2023-10-19T16:44:35.214-04:00",
    "partition": 2,
    "offset": 247283,
    "headers": [
      {
        "key": "task.generation",
        "value": "0"
      },
      {
        "key": "task.id",
        "value": "0"
      },
      {
        "key": "current.iteration",
        "value": "432201"
      }
    ]
  }
```

#### Command
```bash
# Complex Filter using Boolean Query
  kcmd find -b localhost:9092 \
-e "value.quantity == 3881 && value.symbol == 'ZVZZT' && value.userid == 'User_6'"
```


### `grep`
Function finds messages based on regular expressions.
Searches across key, value, message headers
Supports json,avro,string

Usage:
```bash
Usage:
kcmd grep [flags]

Flags:
-a, --apikey string       Optional: APIKey (sasl.username)
-b, --bootstrap string    Bootstrap Server (bootstrap.server)
-e, --expression string   Required: CEL expression for match
-h, --help                help for grep
-i, --input string        Input Format (string[default],json,avro,proto[Not Implemented])
-o, --output string       Type of output (json,table, value[default])
-k, --secretkey string    Optional: SecretKey (sasl.password)
-p, --sr-pass string      Optional: Schema Registry Password
-s, --sr-url string       Optional: Url of Schema Registry
-u, --sr-user string      Optional: Schema Registry Username
-t, --topic string        Required: Topic Name
```
#### Examples
```bash
# Finds a string matching 4164
./kcmd grep -b localhost:9092 -e 4164

```
### `copy`
```bash
Copy messages from one topic to another.
        Optionally filter messages before copying.

Usage:
  kcmd copy [flags]

Flags:
      --c-config string       Path To Consumer Config
  -c, --continuous            Run Command Continously
      --filter string         Filter applied to input topic to filter messages
  -h, --help                  help for copy
  -I, --iarg stringArray      Pass optional Arguments. iargs are passed as -I key=value -X key=value
      --input-bs string       Input Bootstrap Url
      --input-format string   Input format(json,avro,string) - Used to deserialize messages for expression
      --input-topic string    Input Topic Name
  -O, --oarg stringArray      Pass optional Arguments. oargs are passed as -O key=value -X key=value
      --output-bs string      Output Bootstrap Url
      --output-topic string   Output Topic Name
      --p-config string       Path To Producer Config
      --sr-pass string        Schema Registry Password
      --sr-url string         Schema Registry Url
      --sr-user string        Schema Registry User
```
#### Examples
```bash
# Simple Copy Command
./kcmd copy --input-bs localhost:19092 --output-bs localhost:19092 --input-topic twitter-connect-configs --output-topic twitter-connect-offsets

#Continous Copy messages -c|--continuous true will enable consumer keep consuming until closed
./kcmd copy --input-bs localhost:19092 --output-bs localhost:19092 --input-topic twitter-connect-configs --output-topic twitter-connect-offsets -c true

#Complex Example for Confluent Cloud. 
#input-format and filter are set to filter messages before sending to output
./kcmd copy --input-bs  <input-bootstrap-server> \
            --output-bs <output-bootstrap-server> \
            --input-topic datagen-topic \
            --output-topic orders-filtered \
            -I sasl.username=<input-api-key> \
            -I sasl.password=<input-secret-key> \
            -I sasl.mechanism=PLAIN \
            -I security.protocol=SASL_SSL \
            -O sasl.username=<output-api-key> \
            -O sasl.password=<output-api-secret> \
            -O sasl.mechanism=PLAIN \
            -O security.protocol=SASL_SSL \
            --sr-url <schema-registry-url> \
            --sr-user <schema-registry-user> \
            --sr-pass <schema-registry-password> \
            --input-format avro \
            --filter "value.quantity > 4000 && value.side == 'BUY'" 
            
#Copy Complex with continuous and restart
#The group.id is set random string so by passing input arguments overwrites default settings
#Passing input arguments I group.id=FilterOrderedLargerThan4000AndSell -I auto.offset.reset=earliest
#The filter relies on source schema registry so pass in credentials to filter messages using value parameters
#TH
./kcmd copy --input-bs  <input-bootstrap-server> \
            --output-bs <output-bootstrap-server> \
            --input-topic datagen-orders \
            --output-topic large-orders 
            -I group.id=FilterOrderedLargerThan4000AndSell \
            -I auto.offset.reset=earliest \
            --sr-url <schema-registry-url> \
            --sr-user <schema-registry-user> \
            --sr-pass <schema-registry-password> \
            --input-format avro \
            --filter "value.quantity > 4000 && value.side == 'SELL'"
            -c true
```

### `stats`
```bash
Provides statistics such as stats,min,max,avg for topics

Usage:
  kcmd stats [flags]

Flags:
  -a, --apikey string      [Optional] APIKey (sasl.username)
  -b, --bootstrap string   Bootstrap Server (bootstrap.server)
  -f, --filter string      filter topics matching regex
  -h, --help               help for stats
  -o, --output string      Type of output (json,table,key-value)
  -s, --secretkey string   [Optional] SecretKey (sasl.password)
  -X, --xarg stringArray   Pass optional Arguments. xargs are passed as -X key=value -X key=value
```
Examples
```bash
#List all topics and outputs statistics
./kcmd stats -b localhost:9092 -o json

#List all topics and outputs statistics matching regex expression
./kcmd stats -b localhost:9092 -o json -f twitter-*
```

### `count`
```bash
Counts all topic records

Usage:
  kcmd count [flags]

Flags:
  -a, --apikey string      APIKey (sasl.username)
  -b, --bootstrap string   Bootstrap Server (bootstrap.servers)
  -e, --estimate           Estimate the output using watermarks(boolean default false)
  -f, --filter string      filter topics matching regex
  -h, --help               help for count
  -o, --output string      Type of output (json,table,key-value[default])
  -s, --secretkey string   SecretKey (sasl.password)
  -X, --xarg stringArray    Pass optional Arguments. xargs are passed as -X key=value -X key=value
```
#### Examples
```bash
#count all topics
 ./kcmd count -b localhost:9092 -o json

 #simplest example counting  topics where topic matches topic
 ./kcmd count -b localhost:9092 -f "topic" -o json
 
#Filter Count all topics using Confluent Cloud
./kcmd count -b <bootstrap-url> \
               -a <apiKey> \
               -s <secretKey> \ 
               -o json \
               -X sasl.mechanism=PLAIN \
               -X security.protocol=SASL_SSL
           
#Filter Count based on topic matching datagen-topic using Confluent Cloud
./kcmd count -b <bootstrap-url> \
               -a <apiKey> \
               -s <secretKey> \ 
               -f datagen-topic \
               -o json \
               -X sasl.mechanism=PLAIN \
               -X security.protocol=SASL_SSL
               
```
## Group Commands
Provides Consumer Group Commands
### `group list`
List Consumer Group Commands
```bash
List all Consumer Groups

Usage:
  kcmd group list [flags]

Flags:
  -X, --args stringArray   Configuration Argument key=value
  -b, --bootstrap string   Bootstrap Server Url (Required)
  -f, --filter string      Filter groups by value regex
  -h, --help               help for list  
  -o, --output string      Output Table,Json,Print[default]
```
#### Examples
```bash
#List all Consumer Groups and output to json
./kcmd group list -b localhost:9092 -o json

#list consumer groups matching regex zip
./kcmd group list -b localhost:9092 -f "zip"

#Confluent Cloud Example
./kcmd group list \
  -b <bootstrap-server-url> \
  -X sasl.username=<api-key> \
  -X sasl.password=<secret-key \
  -X sasl.mechanism=PLAIN \
  -X security.protocol=SASL_SSL 
```

### `group delete`
Delete Consumer Group(s)
```bash
Delete Consumer Group(s)

Usage:
  kcmd group delete [flags]

Flags:
  -X, --args stringArray    Configuration Argument key=value
  -b, --bootstrap string    Bootstrap Server Url
  -g, --group stringArray   Group Flag for each group to delete
  -h, --help                help for delete
```
#### Examples
```bash
#delete groups
./kcmd group delete -b localhost:9092 -g Group1 -g Group2
```