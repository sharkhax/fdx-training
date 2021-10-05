package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.pipeline.options.PubsubOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class PubsubStringsReader extends PTransform<PBegin, PCollection<String>> {

    @Override
    public PCollection<String> expand(PBegin input) {
        Pipeline pipeline = input.getPipeline();
        PubsubOptions options = pipeline.getOptions().as(PubsubOptions.class);
        ValueProvider<String> subscription = options.getSubscription();
        return input.apply(
                PubsubIO.readStrings().fromSubscription(subscription)
        );
    }
}
