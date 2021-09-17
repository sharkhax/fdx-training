package com.drobot.beam.pipeline;

import com.drobot.beam.option.AvroOptions;
import com.drobot.beam.option.PubsubOptions;
import com.drobot.beam.pipeline.transformation.AvroReadTransform;
import com.drobot.beam.pipeline.transformation.JsonConvertTransform;
import com.drobot.beam.pipeline.transformation.PubsubJsonProducer;
import com.drobot.beam.pipeline.transformation.UserLocationRemoveTransform;
import com.drobot.beam.schema.RecordSchema;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class ProducerPipeline extends Pipeline {

    protected ProducerPipeline(PipelineOptions options) {
        super(options);
    }

    public interface Options extends DataflowPipelineOptions, AvroOptions, PubsubOptions {
    }

    public static Pipeline createPipeline(String... args) {
        Preconditions.checkNotNull(args, "Command line arguments are null");
        PipelineOptionsFactory.register(AvroOptions.class);
        PipelineOptionsFactory.register(PubsubOptions.class);
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .withValidation()
                .create()
                .as(Options.class);
        PipelineRunner.fromOptions(options);
        return new ProducerPipeline(options);
    }

    @Override
    public PipelineResult run() {
        return this.run(getOptions());
    }

    @Override
    public PipelineResult run(PipelineOptions options) {
        Preconditions.checkNotNull(options, "Options reference is null");
        PCollection<GenericRecord> rawRecords = readRecords();
        PCollection<GenericRecord> transformedRecords = removeFields(rawRecords);
        PCollection<String> jsonRecords = convertToJson(transformedRecords);
        publishToPubsub(jsonRecords);
        return super.run(options);
    }

    @Override
    public Options getOptions() {
        return super.getOptions().as(Options.class);
    }

    private PCollection<GenericRecord> readRecords() {
        Schema schema = RecordSchema.getAvroRecordSchema();
        ValueProvider<String> fileTemplate = getOptions().getFileTemplate();
        return super.apply("Read records from avro file", new AvroReadTransform(schema, fileTemplate));
    }

    private PCollection<GenericRecord> removeFields(PCollection<GenericRecord> input) {
        return input.apply("Remove user_location_* fields", new UserLocationRemoveTransform());
    }

    private PCollection<String> convertToJson(PCollection<GenericRecord> input) {
        return input.apply("Serialize to JSON", new JsonConvertTransform());
    }

    @SuppressWarnings("UnusedReturnValue")
    private PDone publishToPubsub(PCollection<String> input) {
        return input.apply("Produce JSON records to pubsub topic", new PubsubJsonProducer());
    }
}
