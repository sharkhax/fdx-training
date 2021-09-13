package com.drobot.beam;

import com.drobot.beam.pipeline.ProducerPipeline;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;

public class Main {

    public static void main(String[] args) {
        Pipeline pipeline = ProducerPipeline.createPipeline(args);
        PipelineResult result = pipeline.run();
        if (pipeline.getOptions().as(DataflowPipelineOptions.class) == null) {
            result.waitUntilFinish();
        }
    }
}
