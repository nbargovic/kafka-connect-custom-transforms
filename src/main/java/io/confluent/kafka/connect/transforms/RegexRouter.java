package io.confluent.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;
import java.util.regex.Pattern;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class RegexRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Route records to a different topic based on a regex match on a field value.";

    private interface ConfigName {
        String FIELD_NAME = "field.name";
        String REGEX = "regex";
        String TOPIC_NAME = "topic.name";
    }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.FIELD_NAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Field name in the record value to match against the regex.")
            .define(ConfigName.REGEX, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Regular expression to match against the field value.")
            .define(ConfigName.TOPIC_NAME, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Target topic name to route to when the regex matches.");

    private static final String PURPOSE = "routing based on regex match";

    private String fieldName;
    private Pattern regex;
    private String topicName;

    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        fieldName = config.getString(ConfigName.FIELD_NAME);
        regex = Pattern.compile(config.getString(ConfigName.REGEX));
        topicName = config.getString(ConfigName.TOPIC_NAME);
    }

    @Override
    public R apply(R record) {
        if (record.valueSchema() == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }
    }

    private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(record.value(), PURPOSE);
        
        Object fieldValue = value.get(fieldName);
        if (fieldValue == null) {
            // Field doesn't exist, keep original topic
            return record;
        }

        String fieldValueStr = fieldValue.toString();
        if (regex.matcher(fieldValueStr).find()) {
            // Regex found in field value, route to configured topic
            return record.newRecord(
                    topicName,
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }

        // No match, keep original topic
        return record;
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);

        final Field field = value.schema().field(fieldName);
        if (field == null) {
            throw new DataException("Field does not exist in the value: " + fieldName);
        }

        Object fieldValue = value.get(field);
        if (fieldValue == null) {
            // Field value is null, keep original topic
            return record;
        }

        String fieldValueStr = fieldValue.toString();
        if (regex.matcher(fieldValueStr).find()) {
            // Regex found in field value, route to configured topic
            return record.newRecord(
                    topicName,
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    record.valueSchema(),
                    record.value(),
                    record.timestamp()
            );
        }

        // No match, keep original topic
        return record;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
        // No resources to clean up
    }
}

