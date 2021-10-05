package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.pipeline.options.JdbcOptions;
import com.drobot.beam.util.PipelineUtil;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;

public class ReadAllTransform<InputT, OutputT> extends PTransform<PCollection<InputT>, PCollection<OutputT>> {

    private final String query;
    private final Coder<OutputT> coder;
    private final JdbcIO.RowMapper<OutputT> rowMapper;
    private final JdbcIO.PreparedStatementSetter<InputT> setter;

    public ReadAllTransform(String query,
                            Coder<OutputT> coder,
                            JdbcIO.RowMapper<OutputT> rowMapper,
                            JdbcIO.PreparedStatementSetter<InputT> setter) {
        this.query = query;
        this.coder = coder;
        this.rowMapper = rowMapper;
        this.setter = setter;
    }

    @Override
    public PCollection<OutputT> expand(PCollection<InputT> input) {
        Pipeline pipeline = input.getPipeline();
        JdbcOptions options = pipeline.getOptions().as(JdbcOptions.class);
        return input.apply(
                JdbcIO.<InputT, OutputT>readAll()
                        .withDataSourceConfiguration(PipelineUtil.createDataSourceConfiguration(options))
                        .withQuery(query)
                        .withParameterSetter(setter)
                        .withRowMapper(rowMapper)
                        .withCoder(coder)
        );
    }
}
