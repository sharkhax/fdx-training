package com.drobot.beam;

import com.drobot.beam.pipeline.AvroToBigQueryWriter;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

public class Main {
    public static void main(String[] args) {
        Pipeline pipeline = AvroToBigQueryWriter.createPipeline(args);
        PipelineResult result = AvroToBigQueryWriter.run(pipeline);
        String templateLocation = pipeline
                .getOptions()
                .as(DataflowPipelineOptions.class)
                .getTemplateLocation();
        if (templateLocation == null || templateLocation.isEmpty()) {
            result.waitUntilFinish();
        }
    }
}
