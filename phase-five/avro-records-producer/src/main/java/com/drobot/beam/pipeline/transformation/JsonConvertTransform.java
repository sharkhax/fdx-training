package com.drobot.beam.pipeline.transformation;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class JsonConvertTransform extends PTransform<PCollection<GenericRecord>, PCollection<String>> {

    private static class JsonConverter extends DoFn<GenericRecord, String> {

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(@Element GenericRecord record, OutputReceiver<String> outputReceiver) {
            String result = encode(record);
            outputReceiver.output(result);
        }

        private String encode(GenericRecord record) {
            return GenericData.get().toString(record);
        }
    }

    @Override
    public PCollection<String> expand(PCollection<GenericRecord> input) {
        return input.apply(ParDo.of(new JsonConverter()));
    }
}
