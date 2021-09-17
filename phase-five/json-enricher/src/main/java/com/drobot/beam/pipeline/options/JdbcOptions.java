package com.drobot.beam.pipeline.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface JdbcOptions extends PipelineOptions {

    @Description("Database username")
    @Default.String("postgres")
    ValueProvider<String> getDbUsername();

    @SuppressWarnings("unused")
    void setDbUsername(ValueProvider<String> dbUsername);

    @Description("Database password")
    @Default.String("postgres")
    ValueProvider<String> getDbPassword();

    @SuppressWarnings("unused")
    void setDbPassword(ValueProvider<String> dbPassword);

    @Description("Url to database")
    @Default.String("jdbc:postgresql:///custom?cloudSqlInstance=phase-one-322509:us-central1:first-instance" +
            "&socketFactory=com.google.cloud.sql.postgres.SocketFactory&user=postgres&password=postgres")
    ValueProvider<String> getDbUrl();

    @SuppressWarnings("unused")
    void setDbUrl(ValueProvider<String> dbUrl);

    @Description("Jdbc driver name")
    @Default.String("org.postgresql.Driver")
    ValueProvider<String> getJdbcDriverName();

    @SuppressWarnings("unused")
    void setJdbcDriverName(ValueProvider<String> jdbcDriverName);
}
