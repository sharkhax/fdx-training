package com.drobot.beam.pipeline;

import com.drobot.beam.pipeline.transformation.SqlAggregationTransform;
import com.drobot.beam.schema.AggregatedRecordSchema;
import com.drobot.beam.schema.RecordSchema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class SqlAggregationTransformTest {

    @Rule public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void udafTest() {
        List<Row> inputRows = buildInputRows();
        List<Row> expectedRows = buildExpectedRows();
        PCollection<Row> output = pipeline
                .apply(Create.of(inputRows))
                .setRowSchema(RecordSchema.getBeamRecordSchema())
                .apply(new SqlAggregationTransform());
        PAssert.that(output).containsInAnyOrder(expectedRows);
        pipeline.run();
    }

    private List<Row> buildInputRows() {
        Row.Builder builder = Row.withSchema(RecordSchema.getBeamRecordSchema());
        Row row1 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 1L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 2)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 4)
                .withFieldValue(RecordSchema.Field.ID, 1L)
                .build();
        Row row2 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 1L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 5)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 0)
                .withFieldValue(RecordSchema.Field.ID, 2L)
                .build();
        Row row3 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 1L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 1)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 3)
                .withFieldValue(RecordSchema.Field.ID, 3L)
                .build();
        Row row4 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 2L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 4)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 10)
                .withFieldValue(RecordSchema.Field.ID, 4L)
                .build();
        Row row5 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 2L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 0)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 0)
                .withFieldValue(RecordSchema.Field.ID, 5L)
                .build();
        Row row6 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 2L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 2)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 1)
                .withFieldValue(RecordSchema.Field.ID, 6L)
                .build();
        Row row7 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 3L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 9)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 10)
                .withFieldValue(RecordSchema.Field.ID, 7L)
                .build();
        Row row8 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 3L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 5)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 10)
                .withFieldValue(RecordSchema.Field.ID, 8L)
                .build();
        Row row9 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 3L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 8)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 10)
                .withFieldValue(RecordSchema.Field.ID, 9L)
                .build();
        Row row10 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 4L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 1)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 5)
                .withFieldValue(RecordSchema.Field.ID, 10L)
                .build();
        Row row11 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 4L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 1)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 1)
                .withFieldValue(RecordSchema.Field.ID, 11L)
                .build();
        Row row12 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 5L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 2)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 7)
                .withFieldValue(RecordSchema.Field.ID, 12L)
                .build();
        return Arrays.asList(row1, row2, row3, row4, row5, row6, row7, row8, row9, row10, row11, row12);
    }

    private List<Row> buildExpectedRows() {
        Row.Builder builder = Row.withSchema(AggregatedRecordSchema.getBeamSchema());
        Row expected1 = builder
                .withFieldValue(AggregatedRecordSchema.Field.HOTEL_ID, 1L)
                .withFieldValue(AggregatedRecordSchema.Field.TOTAL_RECORDS, 3)
                .withFieldValue(AggregatedRecordSchema.Field.ADULTS_CNT, 8)
                .withFieldValue(AggregatedRecordSchema.Field.CHILDREN_CNT, 7)
                .build();
        Row expected2 = builder
                .withFieldValue(AggregatedRecordSchema.Field.HOTEL_ID, 2L)
                .withFieldValue(AggregatedRecordSchema.Field.TOTAL_RECORDS, 3)
                .withFieldValue(AggregatedRecordSchema.Field.ADULTS_CNT, 6)
                .withFieldValue(AggregatedRecordSchema.Field.CHILDREN_CNT, 11)
                .build();
        Row expected3 = builder
                .withFieldValue(AggregatedRecordSchema.Field.HOTEL_ID, 3L)
                .withFieldValue(AggregatedRecordSchema.Field.TOTAL_RECORDS, 3)
                .withFieldValue(AggregatedRecordSchema.Field.ADULTS_CNT, 22)
                .withFieldValue(AggregatedRecordSchema.Field.CHILDREN_CNT, 30)
                .build();
        Row expected4 = builder
                .withFieldValue(AggregatedRecordSchema.Field.HOTEL_ID, 4L)
                .withFieldValue(AggregatedRecordSchema.Field.TOTAL_RECORDS, 2)
                .withFieldValue(AggregatedRecordSchema.Field.ADULTS_CNT, 2)
                .withFieldValue(AggregatedRecordSchema.Field.CHILDREN_CNT, 6)
                .build();
        Row expected5 = builder
                .withFieldValue(AggregatedRecordSchema.Field.HOTEL_ID, 5L)
                .withFieldValue(AggregatedRecordSchema.Field.TOTAL_RECORDS, 1)
                .withFieldValue(AggregatedRecordSchema.Field.ADULTS_CNT, 2)
                .withFieldValue(AggregatedRecordSchema.Field.CHILDREN_CNT, 7)
                .build();
        return Arrays.asList(expected1, expected2, expected3, expected4, expected5);
    }
}
