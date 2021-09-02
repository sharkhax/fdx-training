package com.drobot.cfunc;

public final class ProjectVariable {

    private ProjectVariable() {
    }

    public static final String PROJECT_ID = "phase-one-322509";
    public static final String TEMP_LOCATION = "gs://fdx-training-1/temp";
    public static final String EXTRACT_JOB_NAME = "postgres-to-gcs";
    public static final String TRANSFORM_JOB_NAME = "avro-to-bq-with-aggregation";
    public static final String SCHEDULER_NAME = "phase-two-second";
    public static final String EXTRACT_TEMPLATE_LOCATION = "gs://fdx-training-1/templates/cloudSql-to-gcs";
    public static final String TRANSFORM_TEMPLATE_LOCATION = "gs://fdx-training-1/templates/avro-to-bq-with-aggregation";
    public static final String US_REGION = "us-central1";
    public static final String EU_REGION = "europe-central2";
    public static final String APPLICATION_NAME = "Phase two dataflow templates trigger functions";
    public static final String SCHEDULE = "* * * * *";
    public static final String TIME_ZONE = "Europe/Minsk";
    public static final String EXTRACT_JOB_ID_HEADER = "Extract-Job-Id";
    public static final String SCHEDULER_NAME_HEADER = "Scheduler-Name";
    public static final String TRANSFORM_JOB_TRIGGER_URL = "https://us-central1-phase-one-322509.cloudfunctions.net/transform-job-trigger";
}
