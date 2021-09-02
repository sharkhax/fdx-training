package com.drobot.cfunc.util;

import com.drobot.cfunc.ProjectVariable;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.cloudscheduler.v1.CloudScheduler;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.LaunchTemplateResponse;
import com.google.api.services.dataflow.model.RuntimeEnvironment;

import java.io.IOException;
import java.security.GeneralSecurityException;

public final class TriggerUtil {

    private static final HttpTransport transport;
    private static final JsonFactory jsonFactory;
    private static final GoogleCredential credential;

    static {
        try {
            transport = GoogleNetHttpTransport.newTrustedTransport();
            jsonFactory = JacksonFactory.getDefaultInstance();
            credential = GoogleCredential.getApplicationDefault(transport, jsonFactory);
        } catch (IOException | GeneralSecurityException e) {
            throw new RuntimeException("Error while initializing " + TriggerUtil.class + " class", e);
        }
    }

    private TriggerUtil() {
    }

    public static Dataflow buildDataflow() {
        return new Dataflow.Builder(transport, jsonFactory, credential)
                .setApplicationName(ProjectVariable.APPLICATION_NAME)
                .build();
    }

    public static CloudScheduler buildScheduler() {
        return new CloudScheduler.Builder(transport, jsonFactory, credential)
                .setApplicationName(ProjectVariable.APPLICATION_NAME)
                .build();
    }

    public static RuntimeEnvironment configureRuntimeEnvironment() {
        RuntimeEnvironment runtimeEnvironment = new RuntimeEnvironment();
        runtimeEnvironment.setTempLocation(ProjectVariable.TEMP_LOCATION);
        return runtimeEnvironment;
    }

    public static Job launchTemplate(LaunchTemplateParameters params, String templateLocation, Dataflow dataflow)
            throws IOException {
        Dataflow.Projects.Templates.Launch launch = dataflow.projects().templates().launch(ProjectVariable.PROJECT_ID, params);
        launch.setGcsPath(templateLocation);
        LaunchTemplateResponse response = launch.execute();
        return response.getJob();
    }
}
