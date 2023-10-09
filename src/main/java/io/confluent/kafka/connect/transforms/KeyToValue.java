package io.confluent.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SchemaUtil;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public class KeyToValue<R extends ConnectRecord<R>> implements Transformation<R>, Versioned {

    public static final String OVERVIEW_DOC = "Replace the record key with a new key formed from a subset of fields in the record value.";
    public static final String KEY_FIELD_CONFIG = "keyField";
    public static final String MSG_FIELD_CONFIG = "msgField";
    public static final String DROP_KEY_CONFIG = "dropKey";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(KEY_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Field name in the record key to copy into the record message value.")
            .define(MSG_FIELD_CONFIG, ConfigDef.Type.STRING, ConfigDef.NO_DEFAULT_VALUE, ConfigDef.Importance.HIGH,
                    "Field names in the record message value to copy the key into.")
            .define(DROP_KEY_CONFIG, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.LOW,
                    "If set to true, will set the entire key to null.");

    private static final String PURPOSE = "copying a field from the key to the message value";

    private String keyField;
    private String msgField;
    private boolean dropKey;

    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        keyField = config.getString(KEY_FIELD_CONFIG);
        msgField = config.getString(MSG_FIELD_CONFIG);
        dropKey = config.getBoolean(DROP_KEY_CONFIG);
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
        final Map<String, Object> key = requireMap(record.key(), PURPOSE);

        value.put(msgField, key.get(keyField));

        if(dropKey){
            return record.newRecord(record.topic(), record.kafkaPartition(), null, null, null, value, record.timestamp());
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), null, key, null, value, record.timestamp());
    }

    //this works like org.apache.kafka.connect.transforms.InsertField.applyWithSchema()
    private R applyWithSchema(R record) {
        final Struct value = requireStruct(record.value(), PURPOSE);
        final Struct key = requireStruct(record.key(), PURPOSE);

        Schema valueSchema = value.schema();
        Schema keySchema = key.schema();
        if (valueSchema == null || keySchema == null) {
            throw new DataException("Schema does not exist on the message key or value.");
        }
        final Field fieldFromKey = keySchema.field(keyField);
        if (fieldFromKey == null) {
            throw new DataException("Field does not exist in the key: " + keyField);
        }

        //this updates the record schema.
        //if you are using schema registry, the schema in registry will need to be compatible with this update
        final Field fieldInValue = valueSchema.field(msgField);
        if( fieldInValue == null ) {
            final SchemaBuilder builder = SchemaUtil.copySchemaBasics(valueSchema, SchemaBuilder.struct());
            for (Field field : valueSchema.fields()) {
                builder.field(field.name(), field.schema());
            }
            valueSchema = builder.field(msgField, fieldFromKey.schema()).build();
        }

        //insert the new field
        final Struct updatedValue = new Struct(valueSchema);
        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }
        updatedValue.put(msgField, key.get(keyField));

        if(dropKey){
            return record.newRecord(record.topic(), record.kafkaPartition(), null, null, valueSchema, updatedValue, record.timestamp());
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), keySchema, key, valueSchema, updatedValue, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {

    }

}
