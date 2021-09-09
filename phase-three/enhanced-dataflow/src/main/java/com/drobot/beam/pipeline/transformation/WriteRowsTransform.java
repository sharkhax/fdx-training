package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.pipeline.option.BigQueryCustomOptions;
import com.drobot.beam.schema.AggregatedRecordSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

public class WriteRowsTransform extends PTransform<PCollection<Row>, PDone> {

    @Override
    public PDone expand(PCollection<Row> input) {
        Pipeline pipeline = input.getPipeline();
        BigQueryCustomOptions options = pipeline.getOptions().as(BigQueryCustomOptions.class);
        ValueProvider<String> gcsTempLocation = options.getGcsTempLocation();
        ValueProvider<String> tableReference = options.getTableReference();
        SerializableFunction<Row, TableRow> formatFunction = BigQueryUtils.toTableRow();
        TableSchema schema = AggregatedRecordSchema.getBqTableSchema();
        input.apply("Write aggregated records to BigQuery",
                BigQueryIO.<Row>write()
                        .to(tableReference)
                        .withCustomGcsTempLocation(gcsTempLocation)
                        .withSchema(schema)
                        .withFormatFunction(formatFunction)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
        return PDone.in(pipeline);
    }
}
