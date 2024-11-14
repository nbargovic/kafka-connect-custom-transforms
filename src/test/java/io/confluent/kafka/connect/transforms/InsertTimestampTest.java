package io.confluent.kafka.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
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

import static org.junit.jupiter.api.Assertions.*;

public class InsertTimestampTest {

    private InsertTimestamp<SourceRecord> xform = new InsertTimestamp.Value<>();

    @AfterEach
    public void tearDown() throws Exception {
        xform.close();
    }

    @Test
    public void topLevelStructRequired() {
        assertThrows(DataException.class,
                ()->{
                    xform.configure(Collections.singletonMap("ts.field.name", "il5-timestamp"));
                    xform.apply(new SourceRecord(null, null, "", 0, Schema.INT32_SCHEMA, 42));
                });
    }

    @Test
    public void copySchemaAndInsertTimestampfield() {
        final Map<String, Object> props = new HashMap<>();

        props.put("ts.field.name", "il5-timestamp");

        xform.configure(props);

        final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).build();
        final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

        final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
        final SourceRecord transformedRecord = xform.apply(record);

        assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
        assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
        assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

        assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
        assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
        assertEquals(Schema.INT64_SCHEMA, transformedRecord.valueSchema().field("il5-timestamp").schema());
        assertNotNull(((Struct) transformedRecord.value()).getInt64("il5-timestamp"));

        // Exercise caching
        final SourceRecord transformedRecord2 = xform.apply(
                new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
        assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());

    }

    @Test
    public void schemalessInsertTimestampField() {
        final Map<String, Object> props = new HashMap<>();

        props.put("ts.field.name", "il5-timestamp");

        xform.configure(props);

        final SourceRecord record = new SourceRecord(null, null, "test", 0,
                null, Collections.singletonMap("magic", 42L));

        final SourceRecord transformedRecord = xform.apply(record);
        assertEquals(42L, ((Map) transformedRecord.value()).get("magic"));
        assertNotNull(((Map) transformedRecord.value()).get("il5-timestamp"));

    }

    @Test
    public void copySchemaAndNullValue() {
      final Map<String, Object> props = new HashMap<>();

      props.put("ts.field.name", "il5-timestamp");

      xform.configure(props);

      final Schema simpleStructSchema = SchemaBuilder.struct().name("name").version(1).doc("doc").field("magic", Schema.OPTIONAL_INT64_SCHEMA).field("vars", Schema.STRING_SCHEMA).build();
      final Struct simpleStruct = new Struct(simpleStructSchema).put("magic", 42L);

      final SourceRecord record = new SourceRecord(null, null, "test", 0, simpleStructSchema, simpleStruct);
      final SourceRecord transformedRecord = xform.apply(record);

      assertEquals(simpleStructSchema.name(), transformedRecord.valueSchema().name());
      assertEquals(simpleStructSchema.version(), transformedRecord.valueSchema().version());
      assertEquals(simpleStructSchema.doc(), transformedRecord.valueSchema().doc());

      assertEquals(Schema.OPTIONAL_INT64_SCHEMA, transformedRecord.valueSchema().field("magic").schema());
      assertEquals(42L, ((Struct) transformedRecord.value()).getInt64("magic").longValue());
      assertEquals(Schema.INT64_SCHEMA, transformedRecord.valueSchema().field("il5-timestamp").schema());
      assertNotNull(((Struct) transformedRecord.value()).getInt64("il5-timestamp"));

      // Exercise caching
      final SourceRecord transformedRecord2 = xform.apply(
        new SourceRecord(null, null, "test", 1, simpleStructSchema, new Struct(simpleStructSchema)));
      assertSame(transformedRecord.valueSchema(), transformedRecord2.valueSchema());
  }
}
