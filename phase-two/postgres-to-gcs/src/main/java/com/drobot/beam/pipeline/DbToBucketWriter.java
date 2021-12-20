package com.drobot.beam.pipeline;

import com.drobot.beam.pipeline.transform.JdbcReader;
import com.drobot.beam.schema.SchemaHolder;
import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;

public class DbToBucketWriter {

    private DbToBucketWriter() {
    }

    public interface Options extends DataflowPipelineOptions {

        @Description("Login to database")
        @Default.String("postgres")
        ValueProvider<String> getDbUsername();

        @SuppressWarnings("unused")
        void setDbUsername(ValueProvider<String> dbUsername);

        @Description("Password to database")
        @Default.String("postgres")
        ValueProvider<String> getDbPassword();

        @SuppressWarnings("unused")
        void setDbPassword(ValueProvider<String> dbPassword);

        @Description("JDBC driver name")
        @Default.String("org.postgresql.Driver")
        ValueProvider<String> getJdbcDriverName();

        @SuppressWarnings("unused")
        void setJdbcDriverName(ValueProvider<String> jdbcDriverName);

        @Description("URL to database")
        @Default.String("jdbc:postgresql:///custom?cloudSqlInstance=phase-one-322509:us-central1:first-instance" +
                "&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user=postgres&password=postgres")
        ValueProvider<String> getDbUrl();

        @SuppressWarnings("unused")
        void setDbUrl(ValueProvider<String> dbUrl);

        @Description("Location on gcs where avro file should be written to with its name")
        @Default.String("gs://fdx-training-1/phase-two/new-avro")
        ValueProvider<String> getGcsWritePath();

        @SuppressWarnings("unused")
        void setGcsWritePath(ValueProvider<String> gcsWritePath);
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
        PCollection<GenericRecord> records = readRecordsFromDb(pipeline);
        writeRecordsToBucket(records);
        return pipeline.run();
    }

    private static PCollection<GenericRecord> readRecordsFromDb(Pipeline pipeline) {
        return pipeline.apply(new JdbcReader());
    }

    private static void writeRecordsToBucket(PCollection<GenericRecord> records) {
        Options options = getOptions(records);
        ValueProvider<String> filePath = options.getGcsWritePath();
        records.apply("Write records to bucket",
                AvroIO.writeGenericRecords(SchemaHolder.getAvroRecordSchema())
                        .to(filePath)
                        .withSuffix(".avro")
                );
    }

    private static Options getOptions(Pipeline pipeline) {
        return pipeline.getOptions().as(Options.class);
    }

    private static <T> Options getOptions(PCollection<T> pCollection) {
        return getOptions(pCollection.getPipeline());
    }
}
