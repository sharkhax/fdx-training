package com.drobot.beam.function;

import com.drobot.beam.function.impl.TableSchemaSerDe;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.SerializableFunction;

import java.util.List;

public class GenericRecordToTableRowFunction implements SerializableFunction<GenericRecord, TableRow> {

    private static final SerDe<String, TableSchema> SERDE = new TableSchemaSerDe();
    private final String serializedTableSchema;

    public GenericRecordToTableRowFunction(TableSchema schema) {
        serializedTableSchema = SERDE.serialize(schema);
    }

    @Override
    public TableRow apply(GenericRecord input) {
        TableSchema schema = SERDE.deserialize(serializedTableSchema);
        TableRow result = new TableRow();
        List<TableFieldSchema> fields = schema.getFields();
        for (TableFieldSchema field : fields) {
            String name = field.getName();
            if (field.getType().equals("STRING")) {
                result.set(name, input.get(name).toString());
            } else {
                result.set(name, input.get(name));
            }
        }
        return result;
    }
}
