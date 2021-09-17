package com.drobot.beam.pipeline.options;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubsubOptions extends PipelineOptions {

    @Description("Pubsub subscription name")
    @Default.String("projects/phase-one-322509/subscriptions/phase5")
    ValueProvider<String> getSubscription();

    @SuppressWarnings("unused")
    void setSubscription(ValueProvider<String> subscription);
}
