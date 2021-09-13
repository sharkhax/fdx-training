package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.option.PubsubOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

public class PubsubJsonProducer extends PTransform<PCollection<String>, PDone> {

    @Override
    public PDone expand(PCollection<String> input) {
        Pipeline pipeline = input.getPipeline();
        PubsubOptions options = pipeline.getOptions().as(PubsubOptions.class);
        ValueProvider<String> topic = options.getTopic();
        input.apply("Apply counter metric",
                ParDo.of(new CounterMetricDoFn<>(PubsubJsonProducer.class))
        );
        input.apply(
                PubsubIO.writeStrings()
                        .to(topic)
        );
        return PDone.in(pipeline);
    }
}
