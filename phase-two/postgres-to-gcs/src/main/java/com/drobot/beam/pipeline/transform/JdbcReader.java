package com.drobot.beam.pipeline.transform;

import com.drobot.beam.pipeline.DbToBucketWriter;
import com.drobot.beam.schema.SchemaHolder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

import java.lang.reflect.Field;
import java.sql.ResultSet;

public class JdbcReader extends PTransform<PBegin, PCollection<GenericRecord>> {

    private final String SQL_STATEMENT;

    public JdbcReader() {
        SQL_STATEMENT = buildStatement();
    }

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {
        Pipeline pipeline = input.getPipeline();
        DbToBucketWriter.Options options = pipeline.getOptions().as(DbToBucketWriter.Options.class);
        ValueProvider<String> driverName = options.getJdbcDriverName();
        ValueProvider<String> dbUrl = options.getDbUrl();
        ValueProvider<String> username = options.getDbUsername();
        ValueProvider<String> password = options.getDbPassword();
        return input.apply("Read records from database",
                JdbcIO.<GenericRecord>read()
                        .withDataSourceConfiguration(
                                JdbcIO.DataSourceConfiguration.create(driverName, dbUrl)
                                        .withUsername(username)
                                        .withPassword(password)
                        )
                        .withQuery(SQL_STATEMENT)
                        .withRowMapper(getRowMapper())
                        .withCoder(AvroCoder.of(SchemaHolder.getAvroRecordSchema()))
        );
    }

    private static String buildStatement() {
        Field[] fields = SchemaHolder.Field.class.getFields();
        StringBuilder builder = new StringBuilder("SELECT ");
        try {
            for (int i = 0; i < fields.length - 1; i++) {
                Field field = fields[i];
                builder.append((String) field.get(null)).append(", ");
            }
            builder.append((String) fields[fields.length - 1].get(null)).append(" ");
        } catch (IllegalAccessException e) {
            throw new RuntimeException("Unable to get record's field value", e);
        }
        builder.append("FROM records.raw_records;");
        return builder.toString();
    }

    @SuppressWarnings("Convert2Lambda")
    private static JdbcIO.RowMapper<GenericRecord> getRowMapper() {
        return new JdbcIO.RowMapper<GenericRecord>() {
            @Override
            public GenericRecord mapRow(ResultSet rs) throws Exception {
                GenericRecordBuilder builder = new GenericRecordBuilder(SchemaHolder.getAvroRecordSchema());
                Field[] fields = SchemaHolder.Field.class.getFields();
                for (Field field : fields) {
                    builder.set((String) field.get(null), rs.getObject((String) field.get(null)));
                }
                return builder.build();
            }
        };
    }
}
