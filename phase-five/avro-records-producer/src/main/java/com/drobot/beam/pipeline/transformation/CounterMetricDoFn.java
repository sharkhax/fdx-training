package com.drobot.beam.pipeline.transformation;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class CounterMetricDoFn<T> extends DoFn<T, T> {

    private final Counter counter;
    private static final String METRIC_NAME = "records-counter";

    public CounterMetricDoFn(Class<?> namespace) {
        counter = Metrics.counter(namespace, METRIC_NAME);
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> outputReceiver) {
        counter.inc();
        outputReceiver.output(element);
    }
}
