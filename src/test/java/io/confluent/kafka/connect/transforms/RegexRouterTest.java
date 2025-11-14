package io.confluent.kafka.connect.transforms;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class RegexRouterTest {

    private RegexRouter<SourceRecord> xform = new RegexRouter<>();

    @AfterEach
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test
    public void schemalessMatchRoutes() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "environment");
        props.put("regex", "prod.*");
        props.put("topic.name", "production-topic");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("environment", "production");
        value.put("message", "test");

        final SourceRecord record = new SourceRecord(null, null, "default-topic", 0, null, null, null, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("production-topic", transformedRecord.topic());
        assertEquals(value, transformedRecord.value());
    }

    @Test
    public void schemalessNoMatchKeepsOriginalTopic() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "environment");
        props.put("regex", "prod.*");
        props.put("topic.name", "production-topic");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("environment", "development");
        value.put("message", "test");

        final SourceRecord record = new SourceRecord(null, null, "default-topic", 0, null, null, null, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("default-topic", transformedRecord.topic());
        assertEquals(value, transformedRecord.value());
    }

    @Test
    public void schemalessFieldDoesNotExistKeepsOriginalTopic() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "environment");
        props.put("regex", "prod.*");
        props.put("topic.name", "production-topic");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("message", "test");

        final SourceRecord record = new SourceRecord(null, null, "default-topic", 0, null, null, null, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("default-topic", transformedRecord.topic());
        assertEquals(value, transformedRecord.value());
    }

    @Test
    public void withSchemaMatchRoutes() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "status");
        props.put("regex", "ERROR|CRITICAL");
        props.put("topic.name", "alerts-topic");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("status", Schema.STRING_SCHEMA)
                .field("message", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("status", "ERROR")
                .put("message", "Something went wrong");

        final SourceRecord record = new SourceRecord(null, null, "logs-topic", 0, null, null, schema, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("alerts-topic", transformedRecord.topic());
        assertEquals(value, transformedRecord.value());
    }

    @Test
    public void withSchemaNoMatchKeepsOriginalTopic() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "status");
        props.put("regex", "ERROR|CRITICAL");
        props.put("topic.name", "alerts-topic");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("status", Schema.STRING_SCHEMA)
                .field("message", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("status", "INFO")
                .put("message", "Everything is fine");

        final SourceRecord record = new SourceRecord(null, null, "logs-topic", 0, null, null, schema, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("logs-topic", transformedRecord.topic());
        assertEquals(value, transformedRecord.value());
    }

    @Test
    public void withSchemaFieldDoesNotExistThrowsException() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "nonexistent");
        props.put("regex", ".*");
        props.put("topic.name", "other-topic");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("message", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("message", "test");

        final SourceRecord record = new SourceRecord(null, null, "default-topic", 0, null, null, schema, value);

        DataException exception = assertThrows(DataException.class, () -> xform.apply(record));
        assertTrue(exception.getMessage().contains("Field does not exist in the value: nonexistent"));
    }

    @Test
    public void withSchemaNullFieldValueKeepsOriginalTopic() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "status");
        props.put("regex", "ERROR");
        props.put("topic.name", "alerts-topic");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("status", Schema.OPTIONAL_STRING_SCHEMA)
                .field("message", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("message", "test");

        final SourceRecord record = new SourceRecord(null, null, "logs-topic", 0, null, null, schema, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("logs-topic", transformedRecord.topic());
    }

    @Test
    public void ipAddressRegexMatch() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "host");
        props.put("regex", "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
        props.put("topic.name", "ip-hosts-topic");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("host", "192.168.1.1");
        value.put("port", 8080);

        final SourceRecord record = new SourceRecord(null, null, "hosts-topic", 0, null, null, null, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("ip-hosts-topic", transformedRecord.topic());
    }

    @Test
    public void ipAddressRegexNoMatch() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "host");
        props.put("regex", "\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}");
        props.put("topic.name", "ip-hosts-topic");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("host", "localhost");
        value.put("port", 8080);

        final SourceRecord record = new SourceRecord(null, null, "hosts-topic", 0, null, null, null, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("hosts-topic", transformedRecord.topic());
    }

    @Test
    public void numericFieldValueMatchesRegex() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "code");
        props.put("regex", "5\\d{2}");
        props.put("topic.name", "server-errors-topic");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("code", Schema.INT32_SCHEMA)
                .field("message", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("code", 500)
                .put("message", "Internal Server Error");

        final SourceRecord record = new SourceRecord(null, null, "http-logs-topic", 0, null, null, schema, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("server-errors-topic", transformedRecord.topic());
    }

    @Test
    public void xmlHeaderRegexMatch() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "content");
        props.put("regex", "<\\?xml.*\\?>");
        props.put("topic.name", "xml-messages");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("content", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><data>value</data>");
        value.put("type", "document");

        final SourceRecord record = new SourceRecord(null, null, "raw-messages", 0, null, null, null, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("xml-messages", transformedRecord.topic());
    }

    @Test
    public void xmlHeaderRegexNoMatch() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "content");
        props.put("regex", "<\\?xml.*\\?>");
        props.put("topic.name", "xml-messages");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("content", "{\"key\": \"value\"}");
        value.put("type", "document");

        final SourceRecord record = new SourceRecord(null, null, "raw-messages", 0, null, null, null, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("raw-messages", transformedRecord.topic());
    }

    @Test
    public void xmlHeaderRegexMatchWithSchema() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "content");
        props.put("regex", "<\\?xml.*\\?>");
        props.put("topic.name", "xml-messages");

        xform.configure(props);

        final Schema schema = SchemaBuilder.struct()
                .field("content", Schema.STRING_SCHEMA)
                .field("type", Schema.STRING_SCHEMA)
                .build();

        final Struct value = new Struct(schema)
                .put("content", "<?xml version=\"1.0\"?><root><element>data</element></root>")
                .put("type", "document");

        final SourceRecord record = new SourceRecord(null, null, "raw-messages", 0, null, null, schema, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("xml-messages", transformedRecord.topic());
    }

    @Test
    public void xmlHeaderRegexMatchInMiddleOfContent() {
        final Map<String, String> props = new HashMap<>();
        props.put("field.name", "content");
        props.put("regex", "<\\?xml.*\\?>");
        props.put("topic.name", "xml-messages");

        xform.configure(props);

        final Map<String, Object> value = new HashMap<>();
        value.put("content", "Some text before <?xml version=\"1.0\"?><data>value</data> and text after");
        value.put("type", "document");

        final SourceRecord record = new SourceRecord(null, null, "raw-messages", 0, null, null, null, value);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals("xml-messages", transformedRecord.topic());
    }
}

