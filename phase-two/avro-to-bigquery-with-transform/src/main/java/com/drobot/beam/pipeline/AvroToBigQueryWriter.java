package com.drobot.beam.pipeline;

import com.drobot.beam.schema.AggregatedRecordSchema;
import com.drobot.beam.schema.RecordSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

public class AvroToBigQueryWriter {

    private AvroToBigQueryWriter() {
    }

    public interface Options extends DataflowPipelineOptions {
        @Description("Avro file template")
        @Default.String("gs://fdx-training-1/phase-two/new-avro-*.avro")
        ValueProvider<String> getFileTemplate();

        @SuppressWarnings("unused")
        void setFileTemplate(ValueProvider<String> fileTemplate);

        @Description("BigQuery table reference in format \"project_id:dataset_id.table_id\"")
        @Default.String("phase-one-322509:phase_one.aggregated_records")
        ValueProvider<String> getTableReference();

        @SuppressWarnings("unused")
        void setTableReference(ValueProvider<String> tableReference);

        @Description("Location of GCS to store temporary files")
        @Default.String("gs://fdx-training-1/temp")
        ValueProvider<String> getGcsTempLocation();

        @SuppressWarnings("unused")
        void setGcsTempLocation(ValueProvider<String> gcsTempLocation);
    }

    public static Pipeline createPipeline(String... args) {
        Preconditions.checkNotNull(args, "Command line arguments are null");
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
                .as(Options.class);
        return Pipeline.create(options);
    }

    public static PipelineResult run(Pipeline pipeline) {
        Preconditions.checkNotNull(pipeline, "Pipeline reference is null");
        PCollection<GenericRecord> records = readRecords(pipeline);
        PCollection<Row> rows = mapToRows(records);
        PCollection<Row> transformedRows = makeAggregation(rows);
        writeRows(transformedRows);
        return pipeline.run();
    }

    private static PCollection<GenericRecord> readRecords(Pipeline pipeline) {
        AvroToBigQueryWriter.Options options = getOptions(pipeline);
        ValueProvider<String> gcsUrl = options.getFileTemplate();
        return pipeline.apply("Read avro files",
                AvroIO.readGenericRecords(RecordSchema.getAvroRecordSchema()).from(gcsUrl)
        );
    }

    private static PCollection<Row> mapToRows(PCollection<GenericRecord> records) {
        SerializableFunction<GenericRecord, Row> function =
                AvroUtils.getGenericRecordToRowFunction(RecordSchema.getBeamRecordSchema());
        return records.apply(
                MapElements
                        .into(TypeDescriptor.of(Row.class))
                        .via(function)
        ).setRowSchema(RecordSchema.getBeamRecordSchema());
    }

    private static PCollection<Row> makeAggregation(PCollection<Row> rows) {
        String query = buildSqlQuery();
        return rows.apply("Perform aggregation", SqlTransform.query(query))
                .setRowSchema(AggregatedRecordSchema.getBeamSchema());
    }

    private static void writeRows(PCollection<Row> rows) {
        Options options = getOptions(rows);
        ValueProvider<String> gcsTempLocation = options.getGcsTempLocation();
        ValueProvider<String> tableReference = options.getTableReference();
        SerializableFunction<Row, TableRow> formatFunction = BigQueryUtils.toTableRow();
        TableSchema schema = AggregatedRecordSchema.getBqTableSchema();
        rows.apply("Write aggregated records to BigQuery",
                BigQueryIO.<Row>write()
                        .to(tableReference)
                        .withCustomGcsTempLocation(gcsTempLocation)
                        .withSchema(schema)
                        .withFormatFunction(formatFunction)
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    }

    private static String buildSqlQuery() {
        return "SELECT "
                + RecordSchema.Field.HOTEL_ID + " AS " + AggregatedRecordSchema.Field.HOTEL_ID + ", "
                + "COUNT(*) AS " + AggregatedRecordSchema.Field.TOTAL_RECORDS + ", "
                + "SUM(" + RecordSchema.Field.SRCH_ADULTS_CNT + ") AS " + AggregatedRecordSchema.Field.ADULTS_CNT + ", "
                + "SUM(" + RecordSchema.Field.SRCH_CHILDREN_CNT + ") AS " + AggregatedRecordSchema.Field.CHILDREN_CNT + " "
                + "FROM PCOLLECTION "
                + "GROUP BY " + AggregatedRecordSchema.Field.HOTEL_ID;
    }

    private static Options getOptions(Pipeline pipeline) {
        return pipeline.getOptions().as(Options.class);
    }

    private static <T> Options getOptions(PCollection<T> pCollection) {
        return getOptions(pCollection.getPipeline());
    }
}
