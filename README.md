# KCMD Readme
 
Command Line Utilities for Kafka

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
              -e "Offset > 10000 && Offset < 10010" \ 
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
