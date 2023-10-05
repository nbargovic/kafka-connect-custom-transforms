package io.confluent.kafka.connect.transforms;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class KeyToValueTest {
    private final KeyToValue<SourceRecord> transform = new KeyToValue<>();

    @AfterEach
    public void teardown() {
        transform.close();
    }

    @Test
    public void schemaless() {
        Map<String, String> configs = new HashMap<>();
        configs.put("keyField", "k");
        configs.put("msgField", "v");

        transform.configure(configs);

        final HashMap<String, Integer> key = new HashMap<>();
        key.put("a", 1);
        key.put("k", 2);

        final HashMap<String, Integer> value = new HashMap<>();
        value.put("a", 1);

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, key, null, value);
        final SourceRecord transformedRecord = transform.apply(record);

        final HashMap<String, Integer> expectedValue = new HashMap<>();
        expectedValue.put("a", 1);
        expectedValue.put("v", 2);

        assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void withSchema() {
        Map<String, String> configs = new HashMap<>();
        configs.put("keyField", "k");
        configs.put("msgField", "v");

        transform.configure(configs);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("v", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("a", 1);

        final Schema keySchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("k", Schema.INT32_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema);
        key.put("a", 1);
        key.put("k", 2);

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, keySchema, key, valueSchema, value);
        final SourceRecord transformedRecord = transform.apply(record);

        final Schema expectedKeySchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("k", Schema.INT32_SCHEMA)
                .build();

        final Struct expectedKey = new Struct(expectedKeySchema)
                .put("a", 1)
                .put("k", 2);

        final Schema expectedValueSchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("v", Schema.INT32_SCHEMA)
                .build();

        final Struct expectedValue = new Struct(expectedValueSchema)
                .put("a", 1)
                .put("v", 2);

        assertEquals(expectedKey, transformedRecord.key());
        assertEquals(expectedValue, transformedRecord.value());
    }

    @Test
    public void nonExistingKeyField() {
        Map<String, String> configs = new HashMap<>();
        configs.put("keyField", "no-exist");
        configs.put("msgField", "test");

        transform.configure(configs);

        final Schema keySchema = SchemaBuilder.struct()
                .field("k", Schema.INT32_SCHEMA)
                .build();

        final Schema valueSchema = SchemaBuilder.struct()
                .field("v", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        final Struct key = new Struct(keySchema);
        value.put("v", 1);
        key.put("k", 0);

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, keySchema, key, valueSchema, value);

        DataException actual = assertThrows(DataException.class, () -> transform.apply(record));
        assertEquals("Field does not exist in the key: no-exist", actual.getMessage());
    }

    @Test
    public void missingKeySchema() {
        Map<String, String> configs = new HashMap<>();
        configs.put("keyField", "test");
        configs.put("msgField", "test");

        transform.configure(configs);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("k", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, valueSchema, value);

        DataException actual = assertThrows(DataException.class, () -> transform.apply(record));
    }

    @Test
    public void dropKeyWithSchema() {
        Map<String, String> configs = new HashMap<>();
        configs.put("keyField", "k");
        configs.put("msgField", "v");
        configs.put("dropKey", "true");

        transform.configure(configs);

        final Schema valueSchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("v", Schema.INT32_SCHEMA)
                .build();

        final Struct value = new Struct(valueSchema);
        value.put("a", 1);

        final Schema keySchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("k", Schema.INT32_SCHEMA)
                .build();

        final Struct key = new Struct(keySchema);
        key.put("a", 1);
        key.put("k", 2);

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, keySchema, key, valueSchema, value);
        final SourceRecord transformedRecord = transform.apply(record);

        final Schema expectedValueSchema = SchemaBuilder.struct()
                .field("a", Schema.INT32_SCHEMA)
                .field("v", Schema.INT32_SCHEMA)
                .build();

        final Struct expectedValue = new Struct(expectedValueSchema)
                .put("a", 1)
                .put("v", 2);

        assertEquals(expectedValue, transformedRecord.value());
        assertNull(transformedRecord.key());
    }

    @Test
    public void dropKeySchemaless() {
        Map<String, String> configs = new HashMap<>();
        configs.put("keyField", "k");
        configs.put("msgField", "v");
        configs.put("dropKey", "true");

        transform.configure(configs);

        final HashMap<String, Integer> key = new HashMap<>();
        key.put("a", 1);
        key.put("k", 2);

        final HashMap<String, Integer> value = new HashMap<>();
        value.put("a", 1);

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, key, null, value);
        final SourceRecord transformedRecord = transform.apply(record);

        final HashMap<String, Integer> expectedValue = new HashMap<>();
        expectedValue.put("a", 1);
        expectedValue.put("v", 2);

        assertEquals(expectedValue, transformedRecord.value());
        assertNull(transformedRecord.key());
    }

}
