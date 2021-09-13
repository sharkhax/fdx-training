package com.drobot.beam.pipeline.transformation;

import com.google.gson.Gson;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JsonConvertTransform extends PTransform<PCollection<GenericRecord>, PCollection<String>> {

    private static class JsonConverter extends DoFn<GenericRecord, String> {

        private Gson gson = null;

        @Setup
        public void setUp() {
            gson = new Gson();
        }

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(@Element GenericRecord record, OutputReceiver<String> outputReceiver) {
            Map<String, Object> recordMap = new HashMap<>();
            List<Schema.Field> fields = record.getSchema().getFields();
            for (Schema.Field field : fields) {
                String name = field.name();
                Object value = record.get(name);
                recordMap.put(name, value);
            }
            String json = gson.toJson(recordMap);
            outputReceiver.output(json);
        }
    }

    @Override
    public PCollection<String> expand(PCollection<GenericRecord> input) {
        return input.apply(ParDo.of(new JsonConverter()));
    }
}
