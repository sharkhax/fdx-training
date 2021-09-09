package com.drobot.beam.pipeline.transformation;

import com.drobot.beam.pipeline.udaf.DistinctCount;
import com.drobot.beam.schema.AggregatedRecordSchema;
import com.drobot.beam.schema.RecordSchema;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class SqlAggregationTransform extends PTransform<PCollection<Row>, PCollection<Row>> {

    @Override
    public PCollection<Row> expand(PCollection<Row> input) {
        String query = buildSqlQuery();
        return input.apply("Perform aggregation",
                SqlTransform
                        .query(query)
                        .registerUdaf(DistinctCount.FUNCTION_NAME, new DistinctCount())
        ).setRowSchema(AggregatedRecordSchema.getBeamSchema());
    }

    private static String buildSqlQuery() {
        return "SELECT "
                + RecordSchema.Field.HOTEL_ID + " AS " + AggregatedRecordSchema.Field.HOTEL_ID + ", "
                + DistinctCount.FUNCTION_NAME + "(" + RecordSchema.Field.ID + ") AS " + AggregatedRecordSchema.Field.TOTAL_RECORDS + ", "
                + "SUM(" + RecordSchema.Field.SRCH_ADULTS_CNT + ") AS " + AggregatedRecordSchema.Field.ADULTS_CNT + ", "
                + "SUM(" + RecordSchema.Field.SRCH_CHILDREN_CNT + ") AS " + AggregatedRecordSchema.Field.CHILDREN_CNT + " "
                + "FROM PCOLLECTION "
                + "GROUP BY " + AggregatedRecordSchema.Field.HOTEL_ID;
    }
}
