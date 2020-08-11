package com.miotech.kun.metadata.web.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.miotech.kun.metadata.web.constant.OperatorParam;
import com.miotech.kun.metadata.web.constant.TaskParam;
import com.miotech.kun.metadata.web.constant.WorkflowApiParam;
import com.miotech.kun.metadata.web.util.RequestParameterBuilder;
import com.miotech.kun.workflow.client.WorkflowClient;
import com.miotech.kun.workflow.client.model.Operator;
import com.miotech.kun.workflow.client.model.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.Properties;

@Singleton
public class InitService {
    private static final Logger logger = LoggerFactory.getLogger(InitService.class);

    @Inject
    private WorkflowClient workflowClient;

    @Inject
    private Properties properties;

    public void initDataBuilder() {
        checkOperator(WorkflowApiParam.OPERATOR_NAME_REFRESH, WorkflowApiParam.OPERATOR_NAME_BUILD_ALL);
        checkTask(WorkflowApiParam.TASK_NAME_REFRESH, WorkflowApiParam.TASK_NAME_BUILD_ALL);
        uploadJar();
    }

    private void uploadJar() {
        // Upload jar
    }

    private Optional<Operator> findOperatorByName(String operatorName) {
        return workflowClient.getOperator(operatorName);
    }

    private Optional<Task> findTaskByName(String taskName) {
        return workflowClient.getTask(taskName);
    }

    private void createOperator(String operatorName) {
        Operator operatorOfCreated = workflowClient.saveOperator(operatorName, RequestParameterBuilder.buildOperatorForCreate(operatorName));
        setProp(OperatorParam.get(operatorName).getOperatorKey(), operatorOfCreated.getId().toString());
    }

    private void createTask(String taskName) {
        Task taskOfCreated = workflowClient.createTask(RequestParameterBuilder.buildTaskForCreate(taskName,
                Long.parseLong(properties.getProperty(TaskParam.get(taskName).getOperatorKey())), properties));
        setProp(TaskParam.get(taskName).getTaskKey(), taskOfCreated.getId().toString());
    }

    private void checkOperator(String... operatorNames) {
        for (String operatorName : operatorNames) {
            Optional<Operator> operatorOpt = findOperatorByName(operatorName);
            if (operatorOpt.isPresent()) {
                properties.setProperty(OperatorParam.get(operatorName).getOperatorKey(),
                        operatorOpt.get().getId().toString());
            } else {
                createOperator(operatorName);
                logger.info("Create Operator Success");
            }
        }
    }

    private void checkTask(String... taskNames) {
        for (String taskName : taskNames) {
            Optional<Task> taskOpt = findTaskByName(taskName);
            if (taskOpt.isPresent()) {
                properties.setProperty(TaskParam.get(taskName).getTaskKey(), taskOpt.get().getId().toString());
            } else {
                createTask(taskName);
                logger.info("Create Task Success");
            }
        }
    }

    private void setProp(String key, String value) {
        properties.setProperty(key, value);
    }

}