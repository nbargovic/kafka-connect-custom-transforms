package io.confluent.kafka.connect.predicates;

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class FieldIsIPTest {

    @Test
    public void keyHasIP() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final HashMap<String, String> key = new HashMap<>();
        key.put("host", "192.168.1.1");

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, key, null, null);

        assertTrue(predicate.test(record));
    }

    @Test
    public void keyNotIP() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final HashMap<String, String> key = new HashMap<>();
        key.put("host", "localhost");

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, key, null, null);

        assertFalse(predicate.test(record));
    }

    @Test
    public void keyMissingField() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final HashMap<String, String> key = new HashMap<>();
        key.put("stuff", "foobar");

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, key, null, null);

        assertFalse(predicate.test(record));
    }

    @Test
    public void keyNullField() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final HashMap<String, String> key = new HashMap<>();
        key.put("host", null);

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, key, null, null);

        assertFalse(predicate.test(record));
    }

    @Test
    public void keyIsNull() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, null);

        assertFalse(predicate.test(record));
    }

    @Test
    public void valueHasIP() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        configs.put("useValue", "true");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final HashMap<String, String> value = new HashMap<>();
        value.put("type", "UNKNOWN");
        value.put("host", "192.168.1.1");
        value.put("remoteAddress", "192.168.1.1");

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, value);

        assertTrue(predicate.test(record));
    }

    @Test
    public void valueNotIP() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        configs.put("useValue", "true");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final HashMap<String, String> value = new HashMap<>();
        value.put("type", "RFC3164");
        value.put("host", "localhost");
        value.put("remoteAddress", "192.168.1.1");

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, value);

        assertFalse(predicate.test(record));
    }

    @Test
    public void valueMissingField() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        configs.put("useValue", "true");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final HashMap<String, String> value = new HashMap<>();
        value.put("type", "RFC3164");
        value.put("remoteAddress", "192.168.1.1");

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, value);

        assertFalse(predicate.test(record));
    }

    @Test
    public void valueNullField() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        configs.put("useValue", "true");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final HashMap<String, String> value = new HashMap<>();
        value.put("type", "RFC3164");
        value.put("host", null);
        value.put("remoteAddress", "192.168.1.1");

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, value);

        assertFalse(predicate.test(record));
    }

    @Test
    public void valueIsNull() {
        Map<String, String> configs = new HashMap<>();
        configs.put("field", "host");
        configs.put("useValue", "true");
        FieldIsIP predicate = new FieldIsIP();
        predicate.configure(configs);

        final SourceRecord record = new SourceRecord(null, null, "topic", 0, null, null, null, null);

        assertFalse(predicate.test(record));
    }
}
