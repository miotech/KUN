package com.miotech.kun.dataplatform;

import com.google.common.collect.Lists;
import com.miotech.kun.workflow.client.WorkflowClient;
import com.miotech.kun.workflow.client.model.ConfigKey;
import com.miotech.kun.workflow.client.model.Operator;
import com.miotech.kun.workflow.client.model.Task;
import com.miotech.kun.workflow.client.operator.OperatorUpload;
import com.miotech.kun.workflow.core.execution.ConfigDef;
import com.miotech.kun.workflow.core.model.common.Tag;
import com.miotech.kun.workflow.utils.WorkflowIdGenerator;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

@Configuration
public class TestWorkflowConfig {
    private Operator getMockOperator() {
        ConfigKey configKey = new ConfigKey();
        configKey.setName("sparkSQL");
        configKey.setDisplayName("sql");
        configKey.setReconfigurable(true);
        configKey.setType(ConfigDef.Type.STRING);
        Operator operator = Operator.newBuilder()
                .withId(WorkflowIdGenerator.nextOperatorId())
                .withName("SparkSQL")
                .withClassName("com.miotech.kun.dataplatform.mocking.TestSQLOperator")
                .withConfigDef(Lists.newArrayList(configKey))
                .withDescription("Spark SQL Operator")
                .build();
        return operator;
    }

    @Bean
    public WorkflowClient getWorkflowClient() {
        WorkflowClient mockClient = Mockito.mock(WorkflowClient.class);

        doReturn(getMockOperator())
                .when(mockClient)
                .saveOperator(anyString(), any());

        doReturn(Optional.of(getMockOperator())).when(mockClient).getOperator(anyString());

        doReturn(getMockOperator()).when(mockClient).getOperator(anyLong());

        doAnswer((invocation) -> {
            Task task = invocation.getArgument(0);
            List<Tag> filterTags = invocation.getArgument(1);
            return task.cloneBuilder()
                    .withId(task.getId() == null ? WorkflowIdGenerator.nextTaskId() : task.getId())
                    .withTags(filterTags)
                    .build();
        }).when(mockClient).saveTask(any(), any());

        return mockClient;
    }

    @Bean
    public OperatorUpload getOperatorUpload() {
        OperatorUpload operatorUpload = Mockito.mock(OperatorUpload.class);
        List<Operator> mockOperators = Arrays.asList(getMockOperator());
        doReturn(mockOperators)
                .when(operatorUpload).autoUpload();


        return operatorUpload;
    }
}
