package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.pipeline.options.JdbcOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class DatabaseReader<T> extends PTransform<PBegin, PCollection<T>> {

    private final String statement;
    private final JdbcIO.RowMapper<T> rowMapper;
    private final Coder<T> coder;

    public DatabaseReader(String statement, JdbcIO.RowMapper<T> rowMapper, Coder<T> coder) {
        this.statement = statement;
        this.rowMapper = rowMapper;
        this.coder = coder;
    }

    @Override
    public PCollection<T> expand(PBegin input) {
        Pipeline pipeline = input.getPipeline();
        JdbcOptions options = pipeline.getOptions().as(JdbcOptions.class);
        return input.apply(
                JdbcIO.<T>read()
                        .withDataSourceConfiguration(createDataSourceConfiguration(options))
                        .withQuery(statement)
                        .withRowMapper(rowMapper)
                        .withCoder(coder)
        );
    }

    private JdbcIO.DataSourceConfiguration createDataSourceConfiguration(JdbcOptions options) {
        ValueProvider<String> username = options.getDbUsername();
        ValueProvider<String> password = options.getDbPassword();
        ValueProvider<String> url = options.getDbUrl();
        ValueProvider<String> driver = options.getJdbcDriverName();
        return JdbcIO.DataSourceConfiguration
                .create(driver, url)
                .withUsername(username)
                .withPassword(password);
    }
}
