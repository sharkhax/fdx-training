package com.drobot.beam.pipeline.transformation;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;

public class AvroReadTransform extends PTransform<PBegin, PCollection<GenericRecord>> {

    private final Schema schema;
    private final ValueProvider<String> fileTemplate;

    public AvroReadTransform(Schema schema, ValueProvider<String> fileTemplate) {
        this.schema = schema;
        this.fileTemplate = fileTemplate;
    }

    @Override
    public PCollection<GenericRecord> expand(PBegin input) {
        return input.apply(
                AvroIO.readGenericRecords(schema).from(fileTemplate)
        );
    }
}
