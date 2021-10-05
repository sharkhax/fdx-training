package com.drobot.beam.util;

import com.drobot.beam.pipeline.options.JdbcOptions;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.ValueProvider;

public final class PipelineUtil {

    private PipelineUtil() {
    }

    public static JdbcIO.DataSourceConfiguration createDataSourceConfiguration(JdbcOptions options) {
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
