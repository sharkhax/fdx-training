package com.drobot.beam.function.impl;

import com.drobot.beam.function.SerDe;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryHelpers;

import java.io.Serializable;

public class TableSchemaSerDe implements SerDe<String, TableSchema>, Serializable {

    @Override
    public TableSchema deserialize(String s) {
        return BigQueryHelpers.fromJsonString(s, TableSchema.class);
    }

    @Override
    public String serialize(TableSchema tableSchema) {
        return BigQueryHelpers.toJsonString(tableSchema);
    }
}
