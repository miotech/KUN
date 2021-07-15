package com.miotech.kun.workflow.scheduler;

import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.miotech.kun.commons.utils.EventConsumer;
import com.miotech.kun.commons.utils.EventLoop;
import com.miotech.kun.commons.utils.Props;
import com.miotech.kun.workflow.common.taskrun.bo.TaskAttemptProps;
import com.miotech.kun.workflow.common.taskrun.dao.TaskRunDao;
import com.miotech.kun.workflow.core.Executor;
import com.miotech.kun.workflow.core.event.Event;
import com.miotech.kun.workflow.core.event.TaskAttemptStatusChangeEvent;
import com.miotech.kun.workflow.core.model.taskrun.TaskAttempt;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import com.miotech.kun.workflow.core.model.taskrun.TaskRunStatus;
import com.miotech.kun.workflow.utils.DateTimeUtils;
import com.miotech.kun.workflow.utils.WorkflowIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Singleton
public class TaskManager {
    private static final Logger logger = LoggerFactory.getLogger(TaskManager.class);

    private final Executor executor;

    private final TaskRunDao taskRunDao;

    private final EventBus eventBus;

    private final Props props;

    private InnerEventLoop eventLoop;

    private final Map<Long, Boolean> rerunningTaskRunIds = new ConcurrentHashMap<>();

    private final Integer EVENT_QUEUE_SIZE = 10000;

    private BlockingQueue<TaskRunReadyCheckEvent> taskRunReadyCheckEventQueue = new LinkedBlockingQueue<>(EVENT_QUEUE_SIZE);


    @Inject
    public TaskManager(Executor executor, TaskRunDao taskRunDao, EventBus eventBus, Props props) {
        this.executor = executor;
        this.taskRunDao = taskRunDao;
        this.props = props;

        this.eventLoop = new InnerEventLoop();
        eventLoop.start();

        this.eventBus = eventBus;
        this.eventBus.register(this.eventLoop);
        init();

    }

    /* ----------- public methods ------------ */

    public void submit(List<TaskRun> taskRuns) {
        // 生成对应的TaskAttempt
        List<TaskAttempt> taskAttempts = taskRuns.stream()
                .map(this::createTaskAttempt).collect(Collectors.toList());
        logger.debug("TaskAttempts saved. total={}", taskAttempts.size());
        save(taskAttempts);
        triggerTaskRunReadyCheck();
    }

    /**
     * taskRun status must be finished
     *
     * @param taskRun
     */
    public boolean retry(TaskRun taskRun) {
        checkState(taskRun.getStatus().isFinished(), "taskRun status must be finished ");
        // Does the same re-run request invoked in another threads?
        if (rerunningTaskRunIds.put(taskRun.getId(), Boolean.TRUE) != null) {
            logger.warn("Cannot rerun taskrun instance with id = {}. Reason: another thread is attempting to re-run the same task run.", taskRun.getId());
            return false;
        }
        try {
            TaskAttempt taskAttempt = createTaskAttempt(taskRun, true);
            logger.info("save rerun taskAttempt, taskAttemptId = {}, attempt = {}", taskAttempt.getId(), taskAttempt.getAttempt());
            save(Arrays.asList(taskAttempt));
            updateDownStreamStatus(taskRun.getId(), TaskRunStatus.CREATED);
            triggerTaskRunReadyCheck();
            return true;
        } catch (Exception e) {
            logger.error("Failed to re-run taskrun with id = {} due to exceptions.", taskRun.getId());
            throw e;
        } finally {
            // release the lock
            rerunningTaskRunIds.remove(taskRun.getId());
        }
    }

    /* ----------- private methods ------------ */

    private void init() {
        Thread readyEventConsumer = new Thread(new TaskReadyEventConsumer());
        readyEventConsumer.start();
    }

    private TaskAttempt createTaskAttempt(TaskRun taskRun) {
        return createTaskAttempt(taskRun, false);
    }

    private TaskAttempt createTaskAttempt(TaskRun taskRun, boolean retry) {
        checkNotNull(taskRun, "taskRun should not be null.");
        checkNotNull(taskRun.getId(), "taskRun's id should not be null.");

        TaskAttemptProps savedTaskAttempt = taskRunDao.fetchLatestTaskAttempt(taskRun.getId());

        int attempt = 1;
        if (savedTaskAttempt != null) {
            attempt = retry ? savedTaskAttempt.getAttempt() + 1 : savedTaskAttempt.getAttempt();
        }
        TaskAttempt taskAttempt = TaskAttempt.newBuilder()
                .withId(WorkflowIdGenerator.nextTaskAttemptId(taskRun.getId(), attempt))
                .withTaskRun(taskRun)
                .withAttempt(attempt)
                .withStatus(retry ? TaskRunStatus.CREATED : taskRun.getStatus())
                .withQueueName(taskRun.getQueueName())
                .withPriority(taskRun.getPriority())
                .build();
        logger.debug("Created taskAttempt. taskAttempt={}", taskAttempt);

        return taskAttempt;
    }

    private void save(List<TaskAttempt> taskAttempts) {
        for (TaskAttempt ta : taskAttempts) {
            taskRunDao.createAttempt(ta);
            taskRunDao.updateTaskAttemptStatus(ta.getId(), ta.getStatus());
        }
    }

    private class InnerEventLoop extends EventLoop<Long, Event> {
        public InnerEventLoop() {
            super("task-manager");
            addConsumers(Arrays.asList(new StatusChangeEventConsumer()));
        }

        @Subscribe
        public void onReceive(TaskAttemptStatusChangeEvent event) {
            post(event.getAttemptId(), event);
        }

    }

    private class StatusChangeEventConsumer extends EventConsumer<Long, Event> {
        @Override
        public void onReceive(Event event) {
            if (event instanceof TaskAttemptStatusChangeEvent) {
                TaskAttemptStatusChangeEvent taskAttemptStatusChangeEvent = (TaskAttemptStatusChangeEvent) event;
                TaskRunStatus currentStatus = taskAttemptStatusChangeEvent.getToStatus();
                if (currentStatus.isFinished()) {
                    if (currentStatus.isSuccess()) {
                        triggerTaskRunReadyCheck();
                    } else if (currentStatus.isFailure()) {
                        updateDownStreamStatus(taskAttemptStatusChangeEvent.getTaskRunId(), TaskRunStatus.UPSTREAM_FAILED);
                    }
                }
            }
        }
    }

    private void triggerTaskRunReadyCheck() {
        TaskRunReadyCheckEvent event = new TaskRunReadyCheckEvent(System.currentTimeMillis());
        taskRunReadyCheckEventQueue.offer(event);

    }

    private void submitSatisfyTaskAttemptToExecutor() {
        List<TaskAttempt> taskAttemptList = taskRunDao.fetchAllSatisfyTaskAttempt();
        logger.debug("fetch satisfy taskAttempt size = {}", taskAttemptList.size());
        for (TaskAttempt taskAttempt : taskAttemptList) {
            try {
                executor.submit(taskAttempt);
            } catch (Exception e) {
                logger.warn("submit taskAttempt = {} to executor failed", taskAttempt.getId(), e);
            }
        }
    }

    private void updateDownStreamStatus(Long taskRunId, TaskRunStatus taskRunStatus) {
        boolean usePostgres = postgresEnable();
        List<Long> downStreamTaskRunIds = taskRunDao.fetchDownStreamTaskRunIdsRecursive(taskRunId, usePostgres);
        logger.debug("fetch downStream taskRunIds = {},taskRunId = {}", downStreamTaskRunIds, taskRunId);
        if (taskRunStatus.isTermState()) {
            OffsetDateTime termAt = DateTimeUtils.now();
            taskRunDao.updateAttemptStatusByTaskRunIds(downStreamTaskRunIds, taskRunStatus, termAt);
        } else {
            taskRunDao.updateAttemptStatusByTaskRunIds(downStreamTaskRunIds, taskRunStatus);
        }
    }

    private boolean postgresEnable() {
        String datasourceUrl = props.getString("datasource.jdbcUrl", "");
        return datasourceUrl.contains("postgres");
    }

    private class TaskRunReadyCheckEvent {

        private final long timestamp;

        TaskRunReadyCheckEvent(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    private class TaskReadyEventConsumer implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    List<TaskRunReadyCheckEvent> eventList = new ArrayList<>();
                    taskRunReadyCheckEventQueue.take();
                    taskRunReadyCheckEventQueue.drainTo(eventList);
                    submitSatisfyTaskAttemptToExecutor();
                } catch (Throwable e) {
                    logger.warn("take taskRun ready check event from queue failed");
                }
            }
        }
    }

}
