package com.drobot.beam.pipeline.option;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface BigQueryCustomOptions extends PipelineOptions {

    @Description("BigQuery table reference in format \"project_id:dataset_id.table_id\"")
    @Default.String("phase-one-322509:phase_one.aggregated_records")
    ValueProvider<String> getTableReference();

    @SuppressWarnings("unused")
    void setTableReference(ValueProvider<String> tableReference);

    @Description("Location of GCS to store temporary files")
    @Default.String("gs://fdx-training-1/temp")
    ValueProvider<String> getGcsTempLocation();

    @SuppressWarnings("unused")
    void setGcsTempLocation(ValueProvider<String> gcsTempLocation);
}
