package com.drobot.beam.pipeline;

import com.drobot.beam.pipeline.option.AvroOptions;
import com.drobot.beam.pipeline.option.BigQueryCustomOptions;
import com.drobot.beam.pipeline.transformation.RecordCounterMetric;
import com.drobot.beam.pipeline.transformation.SqlAggregationTransform;
import com.drobot.beam.pipeline.transformation.WriteRowsTransform;
import com.drobot.beam.schema.RecordSchema;
import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

public class AvroToBigQueryWriter {

    private static final String WRITTEN_ROWS_METRIC_NAME = "Custom-counter";

    private AvroToBigQueryWriter() {
    }

    public interface Options extends DataflowPipelineOptions, BigQueryCustomOptions, AvroOptions {
    }

    public static Pipeline createPipeline(String... args) {
        Preconditions.checkNotNull(args, "Command line arguments are null");
        PipelineOptionsFactory.register(Options.class);
        PipelineOptionsFactory.register(BigQueryCustomOptions.class);
        PipelineOptionsFactory.register(AvroOptions.class);
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
        transformedRows = makeMetrics(transformedRows);
        writeRows(transformedRows);
        return pipeline.run();
    }

    private static PCollection<GenericRecord> readRecords(Pipeline pipeline) {
        AvroOptions options = pipeline.getOptions().as(AvroOptions.class);
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
        return rows.apply(new SqlAggregationTransform());
    }

    private static PCollection<Row> makeMetrics(PCollection<Row> rows) {
        DoFn<Row, Row> metricsDoFn = new RecordCounterMetric<>(WRITTEN_ROWS_METRIC_NAME);
        return rows.apply(ParDo.of(metricsDoFn));
    }

    private static void writeRows(PCollection<Row> rows) {
        rows.apply(new WriteRowsTransform());
    }
}
