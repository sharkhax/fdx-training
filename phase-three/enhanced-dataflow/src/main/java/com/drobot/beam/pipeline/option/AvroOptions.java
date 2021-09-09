package com.drobot.beam.pipeline.option;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface AvroOptions extends PipelineOptions {

    @Description("Avro file template")
    @Default.String("gs://fdx-training-1/phase-two/new-avro-*.avro")
    ValueProvider<String> getFileTemplate();

    @SuppressWarnings("unused")
    void setFileTemplate(ValueProvider<String> fileTemplate);
}
