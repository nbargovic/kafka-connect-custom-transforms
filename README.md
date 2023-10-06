# kafka-connect-custom-transforms
Project of custom Kafka Connect transformations that extend the functions available in the Confluent built-in transforms

### KeyToValue

Copies a single record key field to the message value, allows for optionally renaming the field name and/or dropping the key.

This is the opposite of [ValueToKey](https://docs.confluent.io/platform/current/connect/transforms/valuetokey.html).
The user of this transform is responsible for altering the topic schema to support any new message field names.

### Configuration properties

|Name|Description|Type|Default|Valid values|Importance|
|---|---|---|---|---|---|
|`keyField`|Field name in the record key to copy into the record value.|string|-|Any string (json field name)|HIGH
|`msgField`|Destination field name in the record value to copy the key field into.|string|-|Any string (json field name)|HIGH
|`dropKey`|Optionally drop the key after its copied to the message value.|boolean|false|"true" or "false"|OPTIONAL

### Examples

Example 1

```json
"transforms": "KeyToValue",
"transforms.KeyToValue.type":"io.confluent.kafka.connect.transforms.KeyToValue",
"transforms.KeyToValue.keyField": "host"
"transforms.KeyToValue.msgField": "ip"
```

* Message Before: `{ "country": "CZ", "city": "Prague" }`
* Message After: `{ "ip": "192.168.1.1", "country": "CZ", "city": "Prague" }`
* Key Before: `{"host": "192.168.1.1"}`
* Key After: `{"host": "192.168.1.1"}`

Example 2

```json
"transforms": "KeyToValue",
"transforms.KeyToValue.type":"io.confluent.kafka.connect.transforms.KeyToValue",
"transforms.KeyToValue.keyField": "host"
"transforms.KeyToValue.msgField": "ip"
"transforms.KeyToValue.dropKey": "true"
```

* Message Before: `{ "country": "CZ", "city": "Prague" }`
* Message After: `{ "ip": "192.168.1.1", "country": "CZ", "city": "Prague" }`
* Key Before: `{"host": "192.168.1.1"}`
* Key After: `null`

---------

### To Build:

- Requires JDK 11 to build the jar
- Run: `./gradle clean jar`
- Jar file will be generated in `./build/libs/kafka-connect-custom-transforms-1.0.0.jar`

### To Deploy:
Install the custom SMT JAR file into a directory that is under one of the directories listed in the plugin.path property in the Connect worker configuration file as shown below:

`plugin.path=/usr/local/share/kafka/plugins`

For example, create a directory named my-custom-smt under /usr/local/share/kafka/plugins and copy the JAR files into the my-custom-smt directory.

Restart the connect workers, and then try out your custom transformation.

The Connect worker logs each transformation class it finds at the DEBUG level. Enable DEBUG mode and verify that your transformation was found. If not, check the JAR installation and make sure it’s in the correct location.

https://docs.confluent.io/platform/current/connect/transforms/custom.html
