package com.drobot.beam.pipeline.transformation;

import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;

public class CounterMetricDoFn<T> extends DoFn<T, T> {

    private final Counter counter;

    public CounterMetricDoFn(Class<?> namespace, String name) {
        counter = Metrics.counter(namespace, name);
    }

    @SuppressWarnings("unused")
    @ProcessElement
    public void processElement(@Element T element, OutputReceiver<T> outputReceiver) {
        counter.inc();
        outputReceiver.output(element);
    }
}
