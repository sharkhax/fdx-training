package com.drobot.beam.pipeline;

import com.drobot.beam.schema.SchemaHolder;
import com.google.api.services.bigquery.model.TableRow;
import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryUtils;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;

public class AvroToBigQueryWriter {

    private AvroToBigQueryWriter() {
    }

    @SuppressWarnings("unused")
    public interface Options extends PipelineOptions {
        @Description("Avro file template")
        @Default.String("gs://fdx-training-1/test-dataset.avro")
        ValueProvider<String> getFileTemplate();
        void setFileTemplate(ValueProvider<String> fileTemplate);

        @Description("BigQuery table reference in format \"project_id:dataset_id.table_id\"")
        @Default.String("phase-one-322509:phase_one.first_table")
        ValueProvider<String> getTableReference();
        void setTableReference(ValueProvider<String> tableReference);

        @Description("Location of GCS to store temporary files")
        @Default.String("gs://fdx-training-1/temp")
        ValueProvider<String> getGcsTempLocation();
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
        writeRecords(pipeline, records);
        return pipeline.run();
    }

    private static PCollection<GenericRecord> readRecords(Pipeline pipeline) {
        AvroToBigQueryWriter.Options options = getOptions(pipeline);
        ValueProvider<String> gcsUrl = options.getFileTemplate();
        return pipeline.apply("Read avro files",
                AvroIO.readGenericRecords(SchemaHolder.getAvroRecordSchema()).from(gcsUrl)
        );
    }

    private static void writeRecords(Pipeline pipeline, PCollection<GenericRecord> records) {
        Options options = getOptions(pipeline);
        ValueProvider<String> gcsTempLocation = options.getGcsTempLocation();
        ValueProvider<String> tableReference = options.getTableReference();
        records.apply("Write records to BigQuery",
                BigQueryIO.<GenericRecord>write()
                        .to(tableReference)
                        .withCustomGcsTempLocation(gcsTempLocation)
                        .withSchema(SchemaHolder.getBqTableSchema())
                        .withFormatFunction(getRecordToRowFunction())
                        .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                        .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
    }

    private static Options getOptions(Pipeline pipeline) {
        return pipeline.getOptions().as(Options.class);
    }

    private static SerializableFunction<GenericRecord, TableRow> getRecordToRowFunction() {
        return BigQueryUtils.toTableRow(
                AvroUtils.getGenericRecordToRowFunction(SchemaHolder.getBeamRecordSchema())
        );
    }
}
