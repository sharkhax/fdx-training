package com.drobot.beam.pipeline;

import com.drobot.beam.pipeline.udaf.DistinctCount;
import com.drobot.beam.schema.RecordSchema;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DistinctCountTest {

    @Rule
    public final transient TestPipeline pipeline = TestPipeline.create();

    @Test
    public void testTransform() {
        PCollection<Row> rows = pipeline
                .apply(Create.of(buildInitialRows()))
                .setRowSchema(RecordSchema.getBeamRecordSchema())
                .apply(
                        SqlTransform
                                .query("SELECT distinct_count(hotel_id) AS counter FROM PCOLLECTION")
                                .registerUdaf("distinct_count", new DistinctCount())
                );
        PAssert.that(rows).containsInAnyOrder(buildExpectedRow());
        pipeline.run().waitUntilFinish();
    }

    private Row buildExpectedRow() {
        return Row.withSchema(
                Schema.builder().addInt32Field("counter").build()
        ).withFieldValue("counter", 2).build();
    }

    private List<Row> buildInitialRows() {
        Row.Builder builder = Row.withSchema(RecordSchema.getBeamRecordSchema());
        Row row1 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 1L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 2)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 4)
                .build();
        Row row2 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 1L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 2)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 4)
                .build();
        Row row3 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 1L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 1)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 3)
                .build();
        Row row4 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 2L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 4)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 10)
                .build();
        Row row5 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 2L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 0)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 0)
                .build();
        Row row6 = builder
                .withFieldValue(RecordSchema.Field.HOTEL_ID, 2L)
                .withFieldValue(RecordSchema.Field.SRCH_ADULTS_CNT, 0)
                .withFieldValue(RecordSchema.Field.SRCH_CHILDREN_CNT, 0)
                .build();
        return Arrays.asList(row1, row2, row3, row4, row5, row6);
    }
}
