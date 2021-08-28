package com.drobot.beam;

import com.drobot.beam.pipeline.BucketToDbWriter;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

public class Main {
    public static void main(String[] args) {
        Pipeline pipeline = BucketToDbWriter.createPipeline(args);
        PipelineResult result = BucketToDbWriter.run(pipeline);
        String templateLocation = pipeline
                .getOptions()
                .as(DataflowPipelineOptions.class)
                .getTemplateLocation();
        if (templateLocation == null || templateLocation.isEmpty()) {
            result.waitUntilFinish();
        }
    }
}
