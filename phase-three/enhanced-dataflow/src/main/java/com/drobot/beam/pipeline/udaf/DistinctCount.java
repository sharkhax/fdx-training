package com.drobot.beam.pipeline.udaf;

import org.apache.beam.sdk.transforms.Combine;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

public class DistinctCount extends Combine.CombineFn<Long, Set<Long>, Integer> {

    public static final String FUNCTION_NAME = "distinct_count";

    @Override
    public Set<Long> createAccumulator() {
        return new HashSet<>();
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public Set<Long> addInput(Set<Long> mutableAccumulator, Long input) {
        mutableAccumulator.add(input);
        return mutableAccumulator;
    }

    @Override
    public Set<Long> mergeAccumulators(Iterable<Set<Long>> accumulators) {
        Iterator<Set<Long>> iterator = accumulators.iterator();
        Set<Long> merged;
        if (iterator.hasNext()) {
            merged = iterator.next();
            iterator.forEachRemaining(merged::addAll);
        } else {
            merged = createAccumulator();
        }
        return merged;
    }

    @SuppressWarnings("ConstantConditions")
    @Override
    public Integer extractOutput(Set<Long> accumulator) {
        return accumulator.size();
    }
}
