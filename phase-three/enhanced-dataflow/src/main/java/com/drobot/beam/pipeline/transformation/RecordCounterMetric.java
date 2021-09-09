package com.drobot.beam.pipeline.transformation;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class RecordCounterMetric<T> extends DoFn<T, T> {

    private final Counter counter;
    private static final String COUNTER_NAME = "records-count";

    public RecordCounterMetric(String namespace) {
        counter = Metrics.counter(namespace, COUNTER_NAME);
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> output) {
        counter.inc();
        output.output(element);
    }
}
