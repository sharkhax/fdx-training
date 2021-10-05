package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.schema.RecordSchema;
import com.drobot.beam.util.AvroSchemaUtil;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class RecordsEnrichTransform extends PTransform<PCollection<GenericRecord>, PCollection<GenericRecord>> {

    private static final String SQL_STATEMENT = "SELECT "
            + RecordSchema.Field.ID + ", "
            + RecordSchema.Field.USER_LOCATION_COUNTRY + ", "
            + RecordSchema.Field.USER_LOCATION_REGION + ", "
            + RecordSchema.Field.USER_LOCATION_CITY + " "
            + "FROM records.raw_records "
            + "WHERE " + RecordSchema.Field.ID + " = ?";
    private static final Window<KV<Long, GenericRecord>> WINDOWING = Window
            .<KV<Long, GenericRecord>>into(FixedWindows.of(Duration.standardMinutes(1)))
            .withAllowedLateness(Duration.standardMinutes(3))
            .accumulatingFiredPanes();

    private static class ToKvDoFn extends DoFn<GenericRecord, KV<Long, GenericRecord>> {

        private final boolean withTimestamp;

        ToKvDoFn(boolean withTimestamp) {
            this.withTimestamp = withTimestamp;
        }

        @SuppressWarnings("unused")
        @ProcessElement
        public void processElement(@Element GenericRecord input,
                                   OutputReceiver<KV<Long, GenericRecord>> outputReceiver) {
            Long key = (Long) input.get(RecordSchema.Field.ID);
            KV<Long, GenericRecord> output = KV.of(key, input);
            if (withTimestamp) {
                outputReceiver.outputWithTimestamp(output, Instant.now());
            } else {
                outputReceiver.output(output);
            }
        }
    }

    private static class LeftJoinDoFn extends DoFn<KV<Long, CoGbkResult>, GenericRecord> {

        private final TupleTag<GenericRecord> leftTag;
        private final TupleTag<GenericRecord> rightTag;

        LeftJoinDoFn(TupleTag<GenericRecord> leftTag, TupleTag<GenericRecord> rightTag) {
            this.leftTag = leftTag;
            this.rightTag = rightTag;
        }

        @SuppressWarnings({"unused", "ConstantConditions"})
        @ProcessElement
        public void processElement(@Element KV<Long, CoGbkResult> input, OutputReceiver<GenericRecord> outputReceiver) {
            GenericRecord left = input.getValue().getOnly(leftTag, null);
            GenericRecord right = input.getValue().getOnly(rightTag, null);
            GenericRecord output = performLeftJoin(left, right);
            if (output != null) {
                outputReceiver.output(output);
            }
        }

        private GenericRecord performLeftJoin(GenericRecord left, GenericRecord right) {
            GenericRecord result;
            if (left == null) {
                result = null;
            } else if (right == null) {
                result = left;
            } else {
                result = new GenericData().deepCopy(left.getSchema(), left);
                result.put(RecordSchema.Field.USER_LOCATION_COUNTRY, right.get(RecordSchema.Field.USER_LOCATION_COUNTRY));
                result.put(RecordSchema.Field.USER_LOCATION_REGION, right.get(RecordSchema.Field.USER_LOCATION_REGION));
                result.put(RecordSchema.Field.USER_LOCATION_CITY, right.get(RecordSchema.Field.USER_LOCATION_CITY));
            }
            return result;
        }
    }

    @Override
    public PCollection<GenericRecord> expand(PCollection<GenericRecord> input) {
        PCollection<KV<Long, GenericRecord>> kvInput = prepareRawRecords(input);
        PCollection<KV<Long, GenericRecord>> kvUsersData = prepareUsersData(input);
        TupleTag<GenericRecord> rawRecordsTag = new TupleTag<>();
        TupleTag<GenericRecord> usersDataTag = new TupleTag<>();
        PCollection<KV<Long, CoGbkResult>> grouped =
                KeyedPCollectionTuple
                        .of(rawRecordsTag, kvInput)
                        .and(usersDataTag, kvUsersData)
                        .apply("Apply CoGroupByKey", CoGroupByKey.create());
        return grouped
                .apply("Performing join", ParDo.of(new LeftJoinDoFn(rawRecordsTag, usersDataTag)))
                .setCoder(AvroCoder.of(RecordSchema.getAvroRecordSchema()));
    }

    private PCollection<KV<Long, GenericRecord>> prepareRawRecords(PCollection<GenericRecord> input) {
        return input
                .apply("Convert raw records to key value pairs", ParDo.of(new ToKvDoFn(true)))
                .apply("Apply windowing strategy", WINDOWING);
    }

    private PCollection<KV<Long, GenericRecord>> prepareUsersData(PCollection<GenericRecord> rawRecords) {
        JdbcIO.RowMapper<GenericRecord> rowMapper = getRowMapper();
        Coder<GenericRecord> coder = AvroCoder.of(RecordSchema.getAvroRecordSchema());
        JdbcIO.PreparedStatementSetter<GenericRecord> setter = (element, preparedStatement) ->
                preparedStatement.setLong(1, (long) element.get(RecordSchema.Field.ID));
        return rawRecords
                .apply("Read users data", new ReadAllTransform<>(SQL_STATEMENT, coder, rowMapper, setter))
                .apply("Convert users data to key value pairs", ParDo.of(new ToKvDoFn(true)))
                .apply("Apply windowing strategy", WINDOWING);
    }

    private JdbcIO.RowMapper<GenericRecord> getRowMapper() {
        return rs -> {
            GenericRecordBuilder builder =
                    AvroSchemaUtil.createRecordBuilderWithDefaultValues(RecordSchema.getAvroRecordSchema());
            return builder
                    .set(RecordSchema.Field.USER_LOCATION_COUNTRY, rs.getInt(RecordSchema.Field.USER_LOCATION_COUNTRY))
                    .set(RecordSchema.Field.USER_LOCATION_REGION, rs.getInt(RecordSchema.Field.USER_LOCATION_REGION))
                    .set(RecordSchema.Field.USER_LOCATION_CITY, rs.getInt(RecordSchema.Field.USER_LOCATION_CITY))
                    .set(RecordSchema.Field.ID, rs.getLong(RecordSchema.Field.ID))
                    .build();
        };
    }
}
