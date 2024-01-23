# kafka-connect-custom-transforms
Project of custom Kafka Connect transformations that extend the functions available in the Confluent built-in transforms

### Transform - KeyToValue

You dont like the key that a Source Connector creates, so you transformed it, and now you actually want the result in the value? This transform copies a single record key field to the message value (chain this transform if you need to move many fields out of a complex key). This also allows for optionally renaming the field name and/or dropping the key after the copy.

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

* Key Before: `{"host": "192.168.1.1"}`
* Message Before: `{ "country": "CZ", "city": "Prague" }`
* Key After: `{"host": "192.168.1.1"}`
* Message After: `{ "ip": "192.168.1.1", "country": "CZ", "city": "Prague" }`

Example 2

```json
"transforms": "KeyToValue",
"transforms.KeyToValue.type":"io.confluent.kafka.connect.transforms.KeyToValue",
"transforms.KeyToValue.keyField": "host"
"transforms.KeyToValue.msgField": "ip"
"transforms.KeyToValue.dropKey": "true"
```

* Key Before: `{"host": "192.168.1.1"}`
* Message Before: `{ "country": "CZ", "city": "Prague" }`
* Key After: `null`
* Message After: `{ "ip": "192.168.1.1", "country": "CZ", "city": "Prague" }`

---------

### Predicate - FieldValueIsIP
Transformations can be configured with predicates so that the transformation is applied only to records which satisfy a condition. This predicate is intended to be used with the [Hostname Resolver Transformation](https://docs.confluent.io/kafka-connectors/syslog/current/hostname_resolver_transform.html). Use this predicate to only run the hostname transformation when the host is an unresolved IP address. This reduces the amount of reverse DNS lookups by skipping data that the hostname was already found inside the syslog message.

### Configuration properties

|Name|Description|Type|Default|Valid values|Importance|
|---|---|---|---|---|---|
|`field`|Field name in the record key to look for an IP address.|string|-|Any string (json field name)|HIGH
|`useValue`|Optional boolean to use the message value instead of the key to search for an IP address.|boolean|false|Any string (json field name)|LOW

### Examples

Example 1

```json
"transforms": "hostname",
"transforms.hostname.type": "io.confluent.connect.syslog.HostnameResolverTransformation"
"transforms.hostname.predicate": "checkhost"
"predicates": "checkhost"
"predicates.checkhost.type": "io.confluent.kafka.connect.FieldIsIP"
"predicates.checkhost.field": "host"
```

### To Build:

- Requires JDK 11 to build the jar
- Run: `gradle clean jar`
- Jar file will be generated in `./build/libs/kafka-connect-custom-transforms-1.0.0.jar`

### To Deploy:
Install the custom SMT JAR file into a directory that is under one of the directories listed in the plugin.path property in the Connect worker configuration file as shown below:

`plugin.path=/usr/local/share/kafka/plugins`

For example, create a directory named my-custom-smt under /usr/local/share/kafka/plugins and copy the JAR files into the my-custom-smt directory.

Restart the connect workers, and then try out your custom transformation.

The Connect worker logs each transformation class it finds at the DEBUG level. Enable DEBUG mode and verify that your transformation was found. If not, check the JAR installation and make sure it’s in the correct location.

https://docs.confluent.io/platform/current/connect/transforms/custom.html
