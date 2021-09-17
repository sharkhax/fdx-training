package com.drobot.beam.pipeline.transformation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class JsonConvertTransform extends PTransform<PCollection<GenericRecord>, PCollection<String>> {

    private static class JsonConverter extends DoFn<GenericRecord, String> {

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(@Element GenericRecord record, OutputReceiver<String> outputReceiver)
                throws IOException{
            String result = encode(record);
            outputReceiver.output(result);
        }

        private String encode(GenericRecord record) throws IOException {
            Schema schema = record.getSchema();
            try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
                DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
                Encoder encoder = EncoderFactory.get().jsonEncoder(schema, outputStream, false);
                writer.write(record, encoder);
                outputStream.flush();
                encoder.flush();
                return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
            }
        }
    }

    @Override
    public PCollection<String> expand(PCollection<GenericRecord> input) {
        return input.apply(ParDo.of(new JsonConverter()));
    }
}
