package com.drobot.beam.schema;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecordBuilder;

import java.util.List;

public class AvroSchemaUtil {

    private AvroSchemaUtil() {
    }

    public static GenericRecordBuilder createRecordBuilderWithDefaultValues(Schema schema) {
        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        List<Schema.Field> fields = schema.getFields();
        for (Schema.Field field : fields) {
            builder.set(field, field.defaultVal());
        }
        return builder;
    }
}
