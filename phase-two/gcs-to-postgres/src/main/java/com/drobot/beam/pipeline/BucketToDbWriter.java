package com.drobot.beam.pipeline;

import com.drobot.beam.schema.SchemaHolder;
import com.google.common.base.Preconditions;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.values.PCollection;

import java.sql.PreparedStatement;
import java.sql.Types;

public class BucketToDbWriter {

    private static final String SQL_STATEMENT =
            "INSERT INTO records.raw_records VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private BucketToDbWriter() {
    }

    public interface Options extends DataflowPipelineOptions {

        @Description("Avro file template")
        @Default.String("gs://fdx-training-1/test-dataset.avro")
        ValueProvider<String> getFileTemplate();

        @SuppressWarnings("unused")
        void setFileTemplate(ValueProvider<String> fileTemplate);

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
    }

    public static Pipeline createPipeline(String... args) {
        Preconditions.checkNotNull(args, "Command line arguments are null");
        PipelineOptionsFactory.register(Options.class);
        Options options = PipelineOptionsFactory
                .fromArgs(args)
                .create()
				.withValidation()
                .as(Options.class);
        return Pipeline.create(options);
    }

    public static PipelineResult run(Pipeline pipeline) {
        Preconditions.checkNotNull(pipeline, "Pipeline reference is null");
        PCollection<GenericRecord> records = readRecordsFromBucket(pipeline);
        writeRecordsToDatabase(records);
        return pipeline.run();
    }

    private static PCollection<GenericRecord> readRecordsFromBucket(Pipeline pipeline) {
        ValueProvider<String> avroTemplate = getOptions(pipeline).getFileTemplate();
        return pipeline.apply("Read records from bucket",
                AvroIO.readGenericRecords(SchemaHolder.getAvroRecordSchema()).from(avroTemplate));
    }

    private static void writeRecordsToDatabase(PCollection<GenericRecord> records) {
        Options options = getOptions(records);
        ValueProvider<String> dbUrl = options.getDbUrl();
        ValueProvider<String> username = options.getDbUsername();
        ValueProvider<String> password = options.getDbPassword();
        ValueProvider<String> driver = options.getJdbcDriverName();
        JdbcIO.DataSourceConfiguration configuration = JdbcIO.DataSourceConfiguration.create(driver, dbUrl)
                .withUsername(username)
                .withPassword(password);
        records.apply("Write records to database",
                JdbcIO.<GenericRecord>write()
                        .withDataSourceConfiguration(configuration)
                        .withStatement(SQL_STATEMENT)
                        .withPreparedStatementSetter(getStatementSetter())
        );
    }

    @SuppressWarnings("Convert2Lambda")
    private static JdbcIO.PreparedStatementSetter<GenericRecord> getStatementSetter() {
        return new JdbcIO.PreparedStatementSetter<GenericRecord>() {
            @Override
            public void setParameters(GenericRecord element, PreparedStatement preparedStatement) throws Exception {
                preparedStatement.setLong(1, (Long) element.get(SchemaHolder.Field.ID));
                preparedStatement.setString(2, element.get(SchemaHolder.Field.DATE_TIME).toString());
                preparedStatement.setInt(3, (Integer) element.get(SchemaHolder.Field.SITE_NAME));
                preparedStatement.setInt(4, (Integer) element.get(SchemaHolder.Field.POSA_CONTINENT));
                preparedStatement.setInt(5, (Integer) element.get(SchemaHolder.Field.USER_LOCATION_COUNTRY));
                preparedStatement.setInt(6, (Integer) element.get(SchemaHolder.Field.USER_LOCATION_REGION));
                preparedStatement.setInt(7, (Integer) element.get(SchemaHolder.Field.USER_LOCATION_CITY));
                preparedStatement.setObject(8, element.get(SchemaHolder.Field.ORIG_DESTINATION_DISTANCE), Types.DOUBLE);
                preparedStatement.setInt(9, (Integer) element.get(SchemaHolder.Field.USER_ID));
                preparedStatement.setInt(10, (Integer) element.get(SchemaHolder.Field.IS_MOBILE));
                preparedStatement.setInt(11, (Integer) element.get(SchemaHolder.Field.IS_PACKAGE));
                preparedStatement.setInt(12, (Integer) element.get(SchemaHolder.Field.CHANNEL));
                preparedStatement.setString(13, element.get(SchemaHolder.Field.SRCH_CI).toString());
                preparedStatement.setString(14, element.get(SchemaHolder.Field.SRCH_CO).toString());
                preparedStatement.setInt(15, (Integer) element.get(SchemaHolder.Field.SRCH_ADULTS_CNT));
                preparedStatement.setInt(16, (Integer) element.get(SchemaHolder.Field.SRCH_CHILDREN_CNT));
                preparedStatement.setInt(17, (Integer) element.get(SchemaHolder.Field.SRCH_RM_CNT));
                preparedStatement.setInt(18, (Integer) element.get(SchemaHolder.Field.SRCH_DESTINATION_ID));
                preparedStatement.setInt(19, (Integer) element.get(SchemaHolder.Field.SRCH_DESTINATION_TYPE_ID));
                preparedStatement.setLong(20, (Long) element.get(SchemaHolder.Field.HOTEL_ID));
            }
        };
    }

    private static Options getOptions(Pipeline pipeline) {
        return pipeline.getOptions().as(Options.class);
    }

    private static <T> Options getOptions(PCollection<T> pCollection) {
        return getOptions(pCollection.getPipeline());
    }
}
