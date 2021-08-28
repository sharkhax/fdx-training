package com.drobot.beam.schema;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableSchema;
import org.apache.avro.SchemaBuilder;
import org.apache.beam.sdk.schemas.utils.AvroUtils;

import java.util.Arrays;

public class SchemaHolder {

    public static class Field {
        public static final String ID = "id";
        public static final String DATE_TIME = "date_time";
        public static final String SITE_NAME = "site_name";
        public static final String POSA_CONTINENT = "posa_continent";
        public static final String USER_LOCATION_COUNTRY = "user_location_country";
        public static final String USER_LOCATION_REGION = "user_location_region";
        public static final String USER_LOCATION_CITY = "user_location_city";
        public static final String ORIG_DESTINATION_DISTANCE = "orig_destination_distance";
        public static final String USER_ID = "user_id";
        public static final String IS_MOBILE = "is_mobile";
        public static final String IS_PACKAGE = "is_package";
        public static final String CHANNEL = "channel";
        public static final String SRCH_CI = "srch_ci";
        public static final String SRCH_CO = "srch_co";
        public static final String SRCH_ADULTS_CNT = "srch_adults_cnt";
        public static final String SRCH_CHILDREN_CNT = "srch_children_cnt";
        public static final String SRCH_RM_CNT = "srch_rm_cnt";
        public static final String SRCH_DESTINATION_ID = "srch_destination_id";
        public static final String SRCH_DESTINATION_TYPE_ID = "srch_destination_type_id";
        public static final String HOTEL_ID = "hotel_id";
    }

    private static final org.apache.avro.Schema AVRO_RECORD_SCHEMA;
    private static final org.apache.beam.sdk.schemas.Schema BEAM_RECORD_SCHEMA;
    private static final TableSchema BQ_TABLE_SCHEMA;

    private SchemaHolder() {
    }

    static {
        AVRO_RECORD_SCHEMA = createAvroRecordSchema();
        BEAM_RECORD_SCHEMA = createBeamRecordSchema();
        BQ_TABLE_SCHEMA = createBqTableSchema();
    }

    public static org.apache.avro.Schema getAvroRecordSchema() {
        return AVRO_RECORD_SCHEMA;
    }

    @SuppressWarnings("unused")
    public static org.apache.beam.sdk.schemas.Schema getBeamRecordSchema() {
        return BEAM_RECORD_SCHEMA;
    }

    @SuppressWarnings("unused")
    public static TableSchema getBqTableSchema() {
        return BQ_TABLE_SCHEMA;
    }

    private static org.apache.avro.Schema createAvroRecordSchema() {
        return SchemaBuilder.record("topLevelRecord")
                .fields()
                .name(Field.ID).type().nullable().longType().noDefault()
                .name(Field.DATE_TIME).type().nullable().stringType().noDefault()
                .name(Field.SITE_NAME).type().nullable().intType().noDefault()
                .name(Field.POSA_CONTINENT).type().nullable().intType().noDefault()
                .name(Field.USER_LOCATION_COUNTRY).type().nullable().intType().noDefault()
                .name(Field.USER_LOCATION_REGION).type().nullable().intType().noDefault()
                .name(Field.USER_LOCATION_CITY).type().nullable().intType().noDefault()
                .name(Field.ORIG_DESTINATION_DISTANCE).type().nullable().doubleType().noDefault()
                .name(Field.USER_ID).type().nullable().intType().noDefault()
                .name(Field.IS_MOBILE).type().nullable().intType().noDefault()
                .name(Field.IS_PACKAGE).type().nullable().intType().noDefault()
                .name(Field.CHANNEL).type().nullable().intType().noDefault()
                .name(Field.SRCH_CI).type().nullable().stringType().noDefault()
                .name(Field.SRCH_CO).type().nullable().stringType().noDefault()
                .name(Field.SRCH_ADULTS_CNT).type().nullable().intType().noDefault()
                .name(Field.SRCH_CHILDREN_CNT).type().nullable().intType().noDefault()
                .name(Field.SRCH_RM_CNT).type().nullable().intType().noDefault()
                .name(Field.SRCH_DESTINATION_ID).type().nullable().intType().noDefault()
                .name(Field.SRCH_DESTINATION_TYPE_ID).type().nullable().intType().noDefault()
                .name(Field.HOTEL_ID).type().nullable().longType().noDefault()
                .endRecord();
    }

    private static org.apache.beam.sdk.schemas.Schema createBeamRecordSchema() {
        return AvroUtils.toBeamSchema(AVRO_RECORD_SCHEMA);
    }

    private static TableSchema createBqTableSchema() {
        return new TableSchema()
                .setFields(
                        Arrays.asList(
                                new TableFieldSchema().setName(Field.ID).setType("INT64"),
                                new TableFieldSchema().setName(Field.DATE_TIME).setType("STRING"),
                                new TableFieldSchema().setName(Field.SITE_NAME).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.POSA_CONTINENT).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.USER_LOCATION_COUNTRY).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.USER_LOCATION_REGION).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.USER_LOCATION_CITY).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.ORIG_DESTINATION_DISTANCE).setType("FLOAT64"),
                                new TableFieldSchema().setName(Field.USER_ID).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.IS_MOBILE).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.IS_PACKAGE).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.CHANNEL).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.SRCH_CI).setType("STRING"),
                                new TableFieldSchema().setName(Field.SRCH_CO).setType("STRING"),
                                new TableFieldSchema().setName(Field.SRCH_ADULTS_CNT).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.SRCH_CHILDREN_CNT).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.SRCH_RM_CNT).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.SRCH_DESTINATION_ID).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.SRCH_DESTINATION_TYPE_ID).setType("INTEGER"),
                                new TableFieldSchema().setName(Field.HOTEL_ID).setType("INT64")
                        )
                );
    }
}