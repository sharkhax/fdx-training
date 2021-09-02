package com.drobot.cfunc.trigger;

import com.drobot.cfunc.ProjectVariable;
import com.drobot.cfunc.util.TriggerUtil;
import com.google.api.services.dataflow.Dataflow;
import com.google.api.services.dataflow.model.Job;
import com.google.api.services.dataflow.model.LaunchTemplateParameters;
import com.google.api.services.dataflow.model.RuntimeEnvironment;
import com.google.cloud.functions.HttpFunction;
import com.google.cloud.functions.HttpRequest;
import com.google.cloud.functions.HttpResponse;

import java.io.IOException;
import java.util.Date;

@SuppressWarnings("unused")
public class ExtractJobTrigger implements HttpFunction {

    @Override
    public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws IOException {
        Dataflow dataflow = TriggerUtil.buildDataflow();
        RuntimeEnvironment runtimeEnvironment = TriggerUtil.configureRuntimeEnvironment();
        String jobId = launchTemplate(dataflow, runtimeEnvironment);
        CloudSchedulerTrigger schedulerTrigger = CloudSchedulerTrigger.INSTANCE;
        String schedulerName = schedulerTrigger.createScheduler(jobId);
        schedulerTrigger.runScheduler(schedulerName);
    }

    private String launchTemplate(Dataflow dataflow, RuntimeEnvironment runtimeEnvironment) throws IOException {
        String extractJobName = ProjectVariable.EXTRACT_JOB_NAME + "-" + new Date().getTime();
        LaunchTemplateParameters loadJobParams = new LaunchTemplateParameters();
        loadJobParams.setEnvironment(runtimeEnvironment);
        loadJobParams.setJobName(extractJobName);
        Job job = TriggerUtil.launchTemplate(loadJobParams, ProjectVariable.EXTRACT_TEMPLATE_LOCATION, dataflow);
        return job.getId();
    }
}
