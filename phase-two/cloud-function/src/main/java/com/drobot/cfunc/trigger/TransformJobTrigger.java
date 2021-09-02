package com.drobot.cfunc.trigger;

import com.drobot.cfunc.ProjectVariable;
import com.drobot.cfunc.exception.TriggerException;
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
import java.util.List;
import java.util.Map;

@SuppressWarnings("unused")
public class TransformJobTrigger implements HttpFunction {

    @Override
    public void service(HttpRequest httpRequest, HttpResponse httpResponse) throws TriggerException, IOException {
        Dataflow dataflow = TriggerUtil.buildDataflow();
        RuntimeEnvironment runtimeEnvironment = TriggerUtil.configureRuntimeEnvironment();
        Map<String, List<String>> headers = httpRequest.getHeaders();
        String extractJobName;
        String schedulerName;
        try {
            extractJobName = headers.get(ProjectVariable.EXTRACT_JOB_ID_HEADER).get(0);
            schedulerName = headers.get(ProjectVariable.SCHEDULER_NAME_HEADER).get(0);
        } catch (NullPointerException e) {
            throw new TriggerException("Provide " + ProjectVariable.EXTRACT_JOB_ID_HEADER
                    + "and " + ProjectVariable.SCHEDULER_NAME_HEADER + " headers", e);
        }
        if (extractJobName == null || schedulerName == null) {
            throw new TriggerException("Provide " + ProjectVariable.EXTRACT_JOB_ID_HEADER
                    + " (value = " + extractJobName + ") "
                    + "and " + ProjectVariable.SCHEDULER_NAME_HEADER
                    + " (value = " + schedulerName + ") headers");
        }
        String extractJobState = getJobState(dataflow, runtimeEnvironment, extractJobName);
        if (extractJobState.equals("JOB_STATE_DONE")) {
            CloudSchedulerTrigger.INSTANCE.deleteScheduler(schedulerName);
            launchTemplate(dataflow, runtimeEnvironment);
        } else if (!extractJobState.equals("JOB_STATE_RUNNING") && !extractJobState.equals("JOB_STATE_PENDING")) {
            CloudSchedulerTrigger.INSTANCE.deleteScheduler(schedulerName);
        }
    }

    private void launchTemplate(Dataflow dataflow, RuntimeEnvironment runtimeEnvironment) throws IOException {
        String transformJobName = ProjectVariable.TRANSFORM_JOB_NAME + "-" + new Date().getTime();
        LaunchTemplateParameters transformJobParams = new LaunchTemplateParameters();
        transformJobParams.setJobName(transformJobName);
        transformJobParams.setEnvironment(runtimeEnvironment);
        TriggerUtil.launchTemplate(transformJobParams, ProjectVariable.TRANSFORM_TEMPLATE_LOCATION, dataflow);
    }

    private String getJobState(Dataflow dataflow, RuntimeEnvironment runtimeEnvironment, String jobName)
            throws IOException {
        Dataflow.Projects.Locations.Jobs.Get get = dataflow.projects().locations().jobs().get(
                ProjectVariable.PROJECT_ID,
                ProjectVariable.US_REGION,
                jobName
        );
        Job job = get.execute();
        return job.getCurrentState();
    }
}
