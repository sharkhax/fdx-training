package com.drobot.beam.pipeline;

import com.drobot.beam.function.GenericRecordToTableRowFunction;
import com.drobot.beam.pipeline.options.BigQueryOptions;
import com.drobot.beam.pipeline.options.JdbcOptions;
import com.drobot.beam.pipeline.options.PubsubOptions;
import com.drobot.beam.pipeline.transformation.BigQueryWriter;
import com.drobot.beam.pipeline.transformation.JsonGenericRecordConverter;
import com.drobot.beam.pipeline.transformation.PubsubStringsReader;
import com.drobot.beam.pipeline.transformation.RecordsEnrichTransform;
import com.drobot.beam.schema.RecordSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class EnrichingJsonPipeline extends Pipeline {

    private EnrichingJsonPipeline(PipelineOptions options) {
        super(options);
    }

    public interface Options extends DataflowPipelineOptions, PubsubOptions, BigQueryOptions {
    }

    public static Pipeline createPipeline(String... args) {
        Preconditions.checkNotNull(args, "Command line arguments are null");
        PipelineOptionsFactory.register(Options.class);
        PipelineOptionsFactory.register(JdbcOptions.class);
        PipelineOptionsFactory.register(PubsubOptions.class);
        PipelineOptionsFactory.register(BigQueryOptions.class);
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
                .as(Options.class);
        options.setStreaming(true);
        PipelineRunner.fromOptions(options);
        return new EnrichingJsonPipeline(options);
    }

    @Override
    public Options getOptions() {
        return super.getOptions().as(Options.class);
    }

    @Override
    public PipelineResult run() {
        return this.run(getOptions());
    }

    @Override
    public PipelineResult run(PipelineOptions options) {
        Preconditions.checkNotNull(options, "Options reference is null");
        PCollection<String> jsonRecords = readJsonRecords();
        PCollection<GenericRecord> rawRecords = convertToGenericRecord(jsonRecords);
        PCollection<GenericRecord> enrichedRecords = enrichRawData(rawRecords);
        writeRecords(enrichedRecords);
        return super.run(options);
    }

    private PCollection<String> readJsonRecords() {
        return super.apply("Read messages from subscription", new PubsubStringsReader());
    }

    private PCollection<GenericRecord> convertToGenericRecord(PCollection<String> input) {
        return input.apply("Convert json strings to generic records",
                new JsonGenericRecordConverter(RecordSchema.getAvroRecordSchema())
        );
    }

    private PCollection<GenericRecord> enrichRawData(PCollection<GenericRecord> input) {
        return input.apply("Enrich raw records", new RecordsEnrichTransform());
    }

    @SuppressWarnings("UnusedReturnValue")
    private PDone writeRecords(PCollection<GenericRecord> input) {
        TableSchema schema = RecordSchema.getBqTableSchema();
        SerializableFunction<GenericRecord, TableRow> formatFunction = new GenericRecordToTableRowFunction(schema);
        return input.apply("Write records to BigQuery", new BigQueryWriter<>(schema, formatFunction));
    }
}
