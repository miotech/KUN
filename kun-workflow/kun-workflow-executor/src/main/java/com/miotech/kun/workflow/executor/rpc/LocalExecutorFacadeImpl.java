package com.miotech.kun.workflow.executor.rpc;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.miotech.kun.workflow.core.execution.HeartBeatMessage;
import com.miotech.kun.workflow.core.execution.TaskAttemptMsg;
import com.miotech.kun.workflow.executor.ExecutorBackEnd;
import com.miotech.kun.workflow.facade.WorkflowExecutorFacade;

@Singleton
public class LocalExecutorFacadeImpl implements WorkflowExecutorFacade {

    @Inject
    private ExecutorBackEnd executorBackEnd;

    @Override
    public boolean statusUpdate(TaskAttemptMsg attemptMsg) {
        return executorBackEnd.statusUpdate(attemptMsg);

    }

    @Override
    public boolean heartBeat(HeartBeatMessage heartBeatMessage) {
        return executorBackEnd.heartBeatReceive(heartBeatMessage);
    }
}
