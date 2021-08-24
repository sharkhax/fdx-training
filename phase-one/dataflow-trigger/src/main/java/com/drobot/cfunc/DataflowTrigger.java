package com.drobot.cfunc;

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;

import java.io.BufferedWriter;
import java.util.Date;

@SuppressWarnings("unused")
public class DataflowTrigger implements HttpFunction {

    private static final String PROJECT_ID = "phase-one-322509";
    private static final String TEMP_LOCATION = "gs://fdx-training-1/temp";
    private static final String JOB_NAME = "Avro-to-BigQuery-auto";
    private static final String TEMPLATE_LOCATION = "gs://fdx-training-1/templates/avro-to-bigquery";

    @Override
    public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws Exception {
        BufferedWriter writer = httpResponse.getWriter();
        try {
            HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
            JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();
            GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
            Dataflow dataflow = new Dataflow.Builder(transport, jsonFactory, credential)
                    .setApplicationName("Dataflow job trigger")
                    .build();
            RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment();
            runtimeEnvironment.setTempLocation(TEMP_LOCATION);
            LaunchTemplateParameters params = new LaunchTemplateParameters();
            params.setEnvironment(runtimeEnvironment);
            params.setJobName(JOB_NAME + "-" + new Date().getTime());
            Dataflow.Projects.Templates.Launch launch = dataflow.projects().templates().launch(PROJECT_ID, params);
            launch.setGcsPath(TEMPLATE_LOCATION);
            launch.execute();
        } catch (Exception e) {
            writer.write(e.getMessage());
        } finally {
            writer.close();
        }
    }
}
