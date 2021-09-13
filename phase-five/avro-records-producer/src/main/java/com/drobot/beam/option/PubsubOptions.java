package com.drobot.beam.option;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface PubsubOptions extends PipelineOptions {

    @Description("Pubsub topic name")
    @Default.String("projects/phase-one-322509/topics/phase-five")
    ValueProvider<String> getTopic();

    @SuppressWarnings("unused")
    void setTopic(ValueProvider<String> topic);
}
