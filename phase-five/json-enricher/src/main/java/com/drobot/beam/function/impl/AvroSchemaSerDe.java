package com.drobot.beam.function.impl;

import com.drobot.beam.function.SerDe;
import org.apache.avro.Schema;

import java.io.Serializable;

public class AvroSchemaSerDe implements SerDe<String, Schema>, Serializable {

    @Override
    public Schema deserialize(String string) {
        return new Schema.Parser().parse(string);
    }

    @Override
    public String serialize(Schema schema) {
        return schema.toString();
    }
}
