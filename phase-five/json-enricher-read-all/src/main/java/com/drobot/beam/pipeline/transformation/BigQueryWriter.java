package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.function.SerDe;
import com.drobot.beam.function.impl.TableSchemaSerDe;
import com.drobot.beam.pipeline.options.BigQueryOptions;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class BigQueryWriter<T> extends PTransform<PCollection<T>, PDone> {

    private static final SerDe<String, TableSchema> SERDE = new TableSchemaSerDe();

    private final SerializableFunction<T, TableRow> formatFunction;
    private final String serializedTableSchema;

    public BigQueryWriter(TableSchema tableSchema, SerializableFunction<T, TableRow> formatFunction) {
        serializedTableSchema = SERDE.serialize(tableSchema);
        this.formatFunction = formatFunction;
    }

    @Override
    public PDone expand(PCollection<T> input) {
        TableSchema tableSchema = SERDE.deserialize(serializedTableSchema);
        Pipeline pipeline = input.getPipeline();
        BigQueryOptions options = pipeline.getOptions().as(BigQueryOptions.class);
        ValueProvider<String> tableReference = options.getTableReference();
        ValueProvider<String> tempLocation = options.getGcsTempLocation();
        input.apply(
                BigQueryIO.<T>write()
                        .to(tableReference)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND)
                        .withSchema(tableSchema)
                        .withFormatFunction(formatFunction)
                        .withCustomGcsTempLocation(tempLocation)
        );
        return PDone.in(pipeline);
    }
}
