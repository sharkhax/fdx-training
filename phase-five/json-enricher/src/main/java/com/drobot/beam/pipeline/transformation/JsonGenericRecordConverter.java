package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.function.SerDe;
import com.drobot.beam.function.impl.AvroSchemaSerDe;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.io.IOException;

public class JsonGenericRecordConverter extends PTransform<PCollection<String>, PCollection<GenericRecord>> {

    private static final SerDe<String, Schema> SERDE = new AvroSchemaSerDe();

    private final String serializedSchema;

    transient private Schema schema;

    public JsonGenericRecordConverter(Schema schema) {
        serializedSchema = SERDE.serialize(schema);
    }

    private class ConverterDoFn extends DoFn<String, GenericRecord> {

        @SuppressWarnings("unused")
        @Setup
        public void setup() {
            deserializeSchema();
        }

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(@Element String json, OutputReceiver<GenericRecord> outputReceiver)
                throws IOException {
            GenericRecord output = decode(json);
            outputReceiver.output(output);
        }

        private GenericRecord decode(String json) throws IOException {
            DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, json);
            return reader.read(null, decoder);
        }
    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<String> input) {
        deserializeSchema();
        return input.apply(ParDo.of(new ConverterDoFn()))
                .setCoder(AvroCoder.of(schema));
    }

    private void deserializeSchema() {
        if (schema == null) {
            schema = SERDE.deserialize(serializedSchema);
        }
    }
}
