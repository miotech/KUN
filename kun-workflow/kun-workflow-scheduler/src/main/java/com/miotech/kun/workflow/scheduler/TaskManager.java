package com.miotech.kun.workflow.scheduler;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.miotech.kun.workflow.common.taskrun.bo.TaskAttemptProps;
import com.miotech.kun.workflow.common.taskrun.dao.TaskRunDao;
import com.miotech.kun.workflow.core.Executor;
import com.miotech.kun.workflow.core.event.TaskAttemptStatusChangeEvent;
import com.miotech.kun.workflow.core.model.taskrun.TaskAttempt;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import com.miotech.kun.workflow.core.model.taskrun.TaskRunStatus;
import com.miotech.kun.workflow.utils.WorkflowIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Singleton
public class TaskManager {
    private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);

    private final Executor executor;

    private final TaskRunDao taskRunDao;

    private final EventBus eventBus;

    private InnerEventLoop eventLoop;

    private final Map<Long, Object> rerunningTaskRunIds = new ConcurrentHashMap<>();


    @Inject
    public TaskManager(Executor executor, TaskRunDao taskRunDao, EventBus eventBus) {
        this.executor = executor;
        this.taskRunDao = taskRunDao;

        this.eventLoop = new InnerEventLoop();

        this.eventBus = eventBus;
        this.eventBus.register(this.eventLoop);
    }

    /* ----------- public methods ------------ */

    public void submit(List<TaskRun> taskRuns) {
        // 生成对应的TaskAttempt
        List<TaskAttempt> taskAttempts = taskRuns.stream()
                .map(this::createTaskAttempt).collect(Collectors.toList());
        logger.debug("TaskAttempts saved. total={}", taskAttempts.size());
        save(taskAttempts);
        submitSatisfyTaskAttemptToExecutor();
    }

    /**
     * taskRun status must be finished
     *
     * @param taskRun
     */
    public void retry(TaskRun taskRun) {
        checkState(taskRun.getStatus().isFinished(), "taskRun status must be finished ");
        try {
            // Does the same re-run request invoked in another threads?
            if (rerunningTaskRunIds.containsKey(taskRun.getId())) {
                logger.warn("Cannot rerun taskrun instance with id = {}. Reason: another thread is attempting to re-run the same task run.", taskRun.getId());
            }
            rerunningTaskRunIds.put(taskRun.getId(), 1);
        } catch (Exception e) {
            logger.error("Failed to re-run taskrun with id = {} due to exceptions.", taskRun.getId());
            throw e;
        } finally {
            // release the lock
            rerunningTaskRunIds.remove(taskRun.getId());
        }
        TaskAttempt taskAttempt = createTaskAttempt(taskRun);
        logger.info("save rerun taskAttempt, taskAttemptId = {}, attempt = {}", taskAttempt.getId(), taskAttempt.getAttempt());
        save(Arrays.asList(taskAttempt));
        submitSatisfyTaskAttemptToExecutor();
    }

    /* ----------- private methods ------------ */

    private TaskAttempt createTaskAttempt(TaskRun taskRun) {
        checkNotNull(taskRun, "taskRun should not be null.");
        checkNotNull(taskRun.getId(), "taskRun's id should not be null.");

        TaskAttemptProps savedTaskAttempt = taskRunDao.fetchLatestTaskAttempt(taskRun.getId());

        int attempt = 1;
        if (savedTaskAttempt != null) {
            checkState(taskRun.getStatus().isFinished(), "rerun taskAttempt status must be finished ");
            attempt = savedTaskAttempt.getAttempt() + 1;
        }
        TaskAttempt taskAttempt = TaskAttempt.newBuilder()
                .withId(WorkflowIdGenerator.nextTaskAttemptId(taskRun.getId(), attempt))
                .withTaskRun(taskRun)
                .withAttempt(attempt)
                .withStatus(TaskRunStatus.CREATED)
                .withQueueName(taskRun.getQueueName())
                .withPriority(taskRun.getPriority())
                .build();
        logger.debug("Created taskAttempt. taskAttempt={}", taskAttempt);

        return taskAttempt;
    }

    private void save(List<TaskAttempt> taskAttempts) {
        for (TaskAttempt ta : taskAttempts) {
            taskRunDao.createAttempt(ta);
            taskRunDao.updateTaskAttemptStatus(ta.getId(), TaskRunStatus.CREATED);
        }
    }

    private void submitSatisfyTaskAttemptToExecutor() {
        List<TaskAttempt> taskAttemptList = taskRunDao.fetchAllSatisfyTaskAttempt();
        logger.debug("fetch satisfy taskAttempt size = {}", taskAttemptList.size());
        for (TaskAttempt taskAttempt : taskAttemptList) {
            executor.submit(taskAttempt);
        }
    }

    private class InnerEventLoop {

        @Subscribe
        public void onReceive(TaskAttemptStatusChangeEvent event) {
            TaskRunStatus currentStatus = event.getToStatus();
            if (currentStatus.isFinished()) {
                submitSatisfyTaskAttemptToExecutor();
            }
        }
    }

}
