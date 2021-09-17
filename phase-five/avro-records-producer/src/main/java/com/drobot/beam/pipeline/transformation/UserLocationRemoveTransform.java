package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.schema.RecordSchema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class UserLocationRemoveTransform extends PTransform<PCollection<GenericRecord>, PCollection<GenericRecord>> {

    private static class FieldRemover extends DoFn<GenericRecord, GenericRecord> {

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(@Element GenericRecord input, OutputReceiver<GenericRecord> outputReceiver) {
            GenericRecord output = new GenericRecordBuilder((GenericData.Record) input).build();
            output.put(RecordSchema.Field.USER_LOCATION_CITY, null);
            output.put(RecordSchema.Field.USER_LOCATION_REGION, null);
            output.put(RecordSchema.Field.USER_LOCATION_COUNTRY, null);
            outputReceiver.output(output);
        }
    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<GenericRecord> input) {
        return input.apply(ParDo.of(new FieldRemover()));
    }
}
