package com.miotech.kun.workflow.executor;

import com.miotech.kun.workflow.core.model.worker.WorkerInstance;
import com.miotech.kun.workflow.core.model.worker.WorkerSnapshot;
import com.miotech.kun.workflow.core.model.taskrun.TaskAttempt;

import java.util.List;

public interface LifeCycleManager {
    public void start(TaskAttempt taskAttempt);
    public void stop(Long taskAttemptId);
    public WorkerSnapshot get(Long taskAttemptId);
    public List<WorkerInstance> getRunningWorker();
}
