package com.drobot.beam.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.schemas.utils.AvroUtils;

import java.util.Arrays;

public class AggregatedRecordSchema {

    public static class Field {
        public static final String HOTEL_ID = "hotel_id";
        public static final String TOTAL_RECORDS = "total_records";
        public static final String ADULTS_CNT = "adults_cnt";
        public static final String CHILDREN_CNT = "children_cnt";
    }

    private static final org.apache.avro.Schema AVRO_RECORD_SCHEMA;
    private static final org.apache.beam.sdk.schemas.Schema BEAM_RECORD_SCHEMA;
    private static final TableSchema BQ_TABLE_SCHEMA;

    private AggregatedRecordSchema() {
    }

    static {
        AVRO_RECORD_SCHEMA = createAvroRecordSchema();
        BEAM_RECORD_SCHEMA = createBeamRecordSchema();
        BQ_TABLE_SCHEMA = createBqTableSchema();
    }

    @SuppressWarnings("unused")
    public static org.apache.avro.Schema getAvroSchema() {
        return AVRO_RECORD_SCHEMA;
    }

    public static org.apache.beam.sdk.schemas.Schema getBeamSchema() {
        return BEAM_RECORD_SCHEMA;
    }

    public static TableSchema getBqTableSchema() {
        return BQ_TABLE_SCHEMA;
    }

    private static org.apache.avro.Schema createAvroRecordSchema() {
        return SchemaBuilder.record("aggregatedRecord")
                .fields()
                .name(Field.HOTEL_ID).type().nullable().longType().noDefault()
                .name(Field.TOTAL_RECORDS).type().nullable().intType().noDefault()
                .name(Field.ADULTS_CNT).type().nullable().intType().noDefault()
                .name(Field.CHILDREN_CNT).type().nullable().intType().noDefault()
                .endRecord();
    }

    private static org.apache.beam.sdk.schemas.Schema createBeamRecordSchema() {
        return AvroUtils.toBeamSchema(createAvroRecordSchema());
    }

    private static TableSchema createBqTableSchema() {
        return new TableSchema()
                .setFields(
                        Arrays.asList(
                                new TableFieldSchema().setName(Field.HOTEL_ID).setType("INT64"),
                                new TableFieldSchema().setName(Field.TOTAL_RECORDS).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.ADULTS_CNT).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.CHILDREN_CNT).setType("INTEGER")
                        )
                );
    }
}
