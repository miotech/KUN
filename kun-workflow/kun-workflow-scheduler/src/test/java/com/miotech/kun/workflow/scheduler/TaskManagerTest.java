package com.miotech.kun.workflow.scheduler;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.miotech.kun.workflow.common.operator.dao.OperatorDao;
import com.miotech.kun.workflow.common.task.dao.TaskDao;
import com.miotech.kun.workflow.common.taskrun.bo.TaskAttemptProps;
import com.miotech.kun.workflow.common.taskrun.dao.TaskRunDao;
import com.miotech.kun.workflow.core.Executor;
import com.miotech.kun.workflow.core.event.TaskAttemptStatusChangeEvent;
import com.miotech.kun.workflow.core.execution.KunOperator;
import com.miotech.kun.workflow.core.model.operator.Operator;
import com.miotech.kun.workflow.core.model.task.Task;
import com.miotech.kun.workflow.core.model.taskrun.TaskAttempt;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import com.miotech.kun.workflow.core.model.taskrun.TaskRunStatus;
import com.miotech.kun.workflow.testing.factory.MockOperatorFactory;
import com.miotech.kun.workflow.testing.factory.MockTaskAttemptFactory;
import com.miotech.kun.workflow.testing.factory.MockTaskFactory;
import com.miotech.kun.workflow.testing.factory.MockTaskRunFactory;
import com.miotech.kun.workflow.testing.operator.NopOperator;
import com.miotech.kun.workflow.testing.operator.OperatorCompiler;
import com.miotech.kun.workflow.utils.WorkflowIdGenerator;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.inject.Inject;
import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class TaskManagerTest extends SchedulerTestBase {
    @Inject
    private TaskManager taskManager;

    @Inject
    private TaskDao taskDao;

    @Inject
    private TaskRunDao taskRunDao;

    @Inject
    private EventBus eventBus;

    @Inject
    private Executor executor;

    @Inject
    private OperatorDao operatorDao;

    @Override
    protected void configuration() {
        super.configuration();
        executor = mock(Executor.class);
    }

    @Test
    public void testSubmit_task_with_no_dependency() {
        // prepare
        TaskRun taskRun = MockTaskRunFactory.createTaskRun();
        taskDao.create(taskRun.getTask());
        taskRunDao.createTaskRun(taskRun);

        // process
        taskManager.submit(Lists.newArrayList(taskRun));

        // verify
        await().atMost(10, TimeUnit.SECONDS).until(this::invoked);

        List<TaskAttempt> result = getSubmittedTaskAttempts();
        assertThat(result, is(hasSize(1)));

        TaskAttempt taskAttempt = result.get(0);
        assertThat(taskAttempt.getId(), is(WorkflowIdGenerator.nextTaskAttemptId(taskRun.getId(), 1)));
        assertThat(taskAttempt.getAttempt(), is(1));
        checkTaskRun(taskAttempt.getTaskRun(), taskRun);
        assertThat(taskAttempt.getStatus(), is(TaskRunStatus.CREATED));
        assertThat(taskAttempt.getLogPath(), is(nullValue()));
        assertThat(taskAttempt.getStartAt(), is(nullValue()));
        assertThat(taskAttempt.getEndAt(), is(nullValue()));

        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(taskRun.getId());
        assertThat(attemptProps.getId(), is(taskAttempt.getId()));
        // TODO: non-passed yet
        // assertThat(attemptProps.getTaskName(), is(taskAttempt.getTaskName()));
        // assertThat(attemptProps.getTaskId(), is(taskAttempt.getTaskId()));
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.CREATED));
        assertThat(attemptProps.getLogPath(), is(nullValue()));
        assertThat(attemptProps.getStartAt(), is(nullValue()));
        assertThat(attemptProps.getEndAt(), is(nullValue()));
    }

    @Test
    public void testSubmit_task_upstream_is_success() {
        // prepare
        List<Task> tasks = MockTaskFactory.createTasksWithRelations(2, "0>>1");
        List<TaskRun> taskRuns = MockTaskRunFactory.createTaskRunsWithRelations(tasks, "0>>1");
        for (TaskRun taskRun : taskRuns) {
            taskDao.create(taskRun.getTask());
            taskRunDao.createTaskRun(taskRun);
        }

        TaskRun taskRun1 = taskRuns.get(0);
        TaskRun taskRun2 = taskRuns.get(1);

        TaskAttempt attempt1 = MockTaskAttemptFactory.createTaskAttempt(taskRun1);
        taskRunDao.createAttempt(attempt1);
        taskRunDao.updateTaskAttemptStatus(attempt1.getId(), TaskRunStatus.SUCCESS);

        // process
        taskManager.submit(Lists.newArrayList(taskRun2));

        // verify
        await().atMost(10, TimeUnit.SECONDS).until(this::invoked);

        List<TaskAttempt> result = getSubmittedTaskAttempts();
        assertThat(result, is(hasSize(1)));

        TaskAttempt taskAttempt = result.get(0);
        assertThat(taskAttempt.getId(), is(WorkflowIdGenerator.nextTaskAttemptId(taskRun2.getId(), 1)));
        assertThat(taskAttempt.getAttempt(), is(1));
        assertThat(taskAttempt.getTaskRun().getId(), is(taskRun2.getId()));
        checkTaskRun(taskAttempt.getTaskRun(), taskRun2);
        assertThat(taskAttempt.getStatus(), is(TaskRunStatus.CREATED));
        assertThat(taskAttempt.getLogPath(), is(nullValue()));
        assertThat(taskAttempt.getStartAt(), is(nullValue()));
        assertThat(taskAttempt.getEndAt(), is(nullValue()));
    }

    @Test
    public void testSubmit_task_upstream_is_failed() throws InterruptedException {
        // prepare
        List<Task> tasks = MockTaskFactory.createTasksWithRelations(2, "0>>1");
        List<TaskRun> taskRuns = MockTaskRunFactory.createTaskRunsWithRelations(tasks, "0>>1");
        for (TaskRun taskRun : taskRuns) {
            taskDao.create(taskRun.getTask());
            taskRunDao.createTaskRun(taskRun);
        }

        TaskRun taskRun1 = taskRuns.get(0);
        TaskRun taskRun2 = taskRuns.get(1);

        TaskAttempt attempt1 = MockTaskAttemptFactory.createTaskAttempt(taskRun1);
        taskRunDao.createAttempt(attempt1);
        taskRunDao.updateTaskAttemptStatus(attempt1.getId(), TaskRunStatus.FAILED);

        // process
        taskManager.submit(Lists.newArrayList(taskRun2));

        // verify
        TimeUnit.SECONDS.sleep(2);
        assertThat(invoked(), is(false));
    }

    @Test
    public void testSubmit_task_waiting_for_upstream_becomes_success() throws InterruptedException {
        // prepare
        List<Task> tasks = MockTaskFactory.createTasksWithRelations(2, "0>>1");
        List<TaskRun> taskRuns = MockTaskRunFactory.createTaskRunsWithRelations(tasks, "0>>1");
        for (TaskRun taskRun : taskRuns) {
            taskDao.create(taskRun.getTask());
            taskRunDao.createTaskRun(taskRun);
        }

        TaskRun taskRun1 = taskRuns.get(0);
        TaskRun taskRun2 = taskRuns.get(1);

        TaskAttempt attempt1 = MockTaskAttemptFactory.createTaskAttempt(taskRun1);
        taskRunDao.createAttempt(attempt1);
        taskRunDao.updateTaskAttemptStatus(attempt1.getId(), TaskRunStatus.RUNNING);

        // process
        taskManager.submit(Lists.newArrayList(taskRun2));
        TimeUnit.SECONDS.sleep(2);
        assertThat(invoked(), is(false));

        //
        taskRunDao.updateTaskAttemptStatus(attempt1.getId(), TaskRunStatus.SUCCESS, null, null);
        // post successful event
        eventBus.post(new TaskAttemptStatusChangeEvent(attempt1.getId(), TaskRunStatus.RUNNING, TaskRunStatus.SUCCESS, "test task", 0L));

        // verify
        await().atMost(10, TimeUnit.SECONDS).until(this::invoked);

        List<TaskAttempt> result = getSubmittedTaskAttempts();
        assertThat(result, is(hasSize(1)));

        TaskAttempt taskAttempt = result.get(0);
        assertThat(taskAttempt.getId(), is(WorkflowIdGenerator.nextTaskAttemptId(taskRun2.getId(), 1)));
        assertThat(taskAttempt.getAttempt(), is(1));
        checkTaskRun(taskAttempt.getTaskRun(), taskRun2);
        assertThat(taskAttempt.getStatus(), is(TaskRunStatus.CREATED));
        assertThat(taskAttempt.getLogPath(), is(nullValue()));
        assertThat(taskAttempt.getStartAt(), is(nullValue()));
        assertThat(taskAttempt.getEndAt(), is(nullValue()));
    }

    private void checkTaskRun(TaskRun actual, TaskRun except) {
        assertThat(actual.getId(), is(except.getId()));
        assertThat(actual.getPriority(), is(except.getPriority()));
        assertThat(actual.getQueueName(), is(except.getQueueName()));
        assertThat(actual.getScheduledTick(), is(except.getScheduledTick()));
        assertThat(actual.getScheduledType(), is(except.getScheduledType()));
    }

    @Test
    public void rerunTaskRunShouldInvokeDownStreamTaskRun() {

        List<Task> taskList = MockTaskFactory.createTasksWithRelations(2, "0>>1");

        long operatorId = taskList.get(0).getOperatorId();
        Operator op = MockOperatorFactory.createOperator()
                .cloneBuilder()
                .withId(operatorId)
                .withName("Operator_" + operatorId)
                .withClassName("testOperator")
                .withPackagePath(compileJar(NopOperator.class, NopOperator.class.getSimpleName()))
                .build();
        operatorDao.createWithId(op, operatorId);
        taskList.forEach(task -> taskDao.create(task));
        List<TaskRun> taskRunList = MockTaskRunFactory.createTaskRunsWithRelations(taskList, "0>>1");
        TaskRun taskRun1 = taskRunList.get(0).cloneBuilder().withStatus(TaskRunStatus.FAILED).build();
        TaskRun taskRun2 = taskRunList.get(1);
        taskRunDao.createTaskRun(taskRun1);
        taskRunDao.createTaskRun(taskRun2);
        TaskAttempt taskAttempt = TaskAttempt.newBuilder()
                .withId(WorkflowIdGenerator.nextTaskAttemptId(taskRun1.getId(), 1))
                .withTaskRun(taskRun1)
                .withAttempt(1)
                .withStatus(TaskRunStatus.FAILED)
                .withQueueName(taskRun1.getQueueName())
                .withPriority(taskRun1.getPriority())
                .build();
        taskRunDao.createAttempt(taskAttempt);
        taskManager.submit(Arrays.asList(taskRun2));
//        long rerunAttemptId = taskRun1.getId() + 2;
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                TaskAttempt taskAttempt = invocation.getArgument(0, TaskAttempt.class);
                taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.SUCCESS);
                eventBus.post(prepareEvent(taskAttempt.getId(), taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.SUCCESS));
                return null;
            }
        }).when(executor).submit(ArgumentMatchers.any());

        //retry taskRun1
        taskManager.retry(taskRun1);


        // verify invoke downStream

        awaitUntilAttemptDone(taskRun2.getId() + 1);

        TaskAttemptProps attemptProps2 = taskRunDao.fetchLatestTaskAttempt(taskRun2.getId());
        assertThat(attemptProps2.getId(), is(attemptProps2.getId()));
        assertThat(attemptProps2.getAttempt(), is(1));
        assertThat(attemptProps2.getStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(attemptProps2.getLogPath(), is(nullValue()));
        assertThat(attemptProps2.getStartAt(), is(nullValue()));
        assertThat(attemptProps2.getEndAt(), is(nullValue()));


    }

    @Test
    public void upstreamFailedShouldUpdateDownstreamStatus() {

        List<Task> taskList = MockTaskFactory.createTasksWithRelations(3, "0>>1;1>>2");

        long operatorId = taskList.get(0).getOperatorId();
        Operator op = MockOperatorFactory.createOperator()
                .cloneBuilder()
                .withId(operatorId)
                .withName("Operator_" + operatorId)
                .withClassName("testOperator")
                .withPackagePath(compileJar(NopOperator.class, NopOperator.class.getSimpleName()))
                .build();
        operatorDao.createWithId(op, operatorId);
        taskList.forEach(task -> taskDao.create(task));
        List<TaskRun> taskRunList = MockTaskRunFactory.createTaskRunsWithRelations(taskList, "0>>1;1>>2");
        TaskRun taskRun1 = taskRunList.get(0);
        TaskRun taskRun2 = taskRunList.get(1);
        TaskRun taskRun3 = taskRunList.get(2);
        taskRunDao.createTaskRun(taskRun1);
        taskRunDao.createTaskRun(taskRun2);
        taskRunDao.createTaskRun(taskRun3);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                System.out.println("do answer...");
                TaskAttempt taskAttempt = invocation.getArgument(0, TaskAttempt.class);
                if (taskAttempt.getTaskRun().getId().equals(taskRun1.getId())) {
                    taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.FAILED);
                    eventBus.post(prepareEvent(taskAttempt.getId()
                            , taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.FAILED));
                } else {
                    taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.SUCCESS);
                    eventBus.post(prepareEvent(taskAttempt.getId()
                            , taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.SUCCESS));
                }
                return null;
            }
        }).when(executor).submit(ArgumentMatchers.any());
        taskManager.submit(taskRunList);


        awaitUntilAttemptDone(taskRun1.getId() + 1);

        // verify update downStream
        TaskAttemptProps attemptProps1 = taskRunDao.fetchLatestTaskAttempt(taskRun1.getId());
        assertThat(attemptProps1.getAttempt(), is(1));
        assertThat(attemptProps1.getStatus(), is(TaskRunStatus.FAILED));
        assertThat(attemptProps1.getLogPath(), is(nullValue()));
        assertThat(attemptProps1.getStartAt(), is(nullValue()));
        assertThat(attemptProps1.getEndAt(), is(nullValue()));

        TaskAttemptProps attemptProps2 = taskRunDao.fetchLatestTaskAttempt(taskRun2.getId());
        assertThat(attemptProps2.getAttempt(), is(1));
        assertThat(attemptProps2.getStatus(), is(TaskRunStatus.UPSTREAM_FAILED));
        assertThat(attemptProps2.getLogPath(), is(nullValue()));
        assertThat(attemptProps2.getStartAt(), is(nullValue()));
        assertThat(attemptProps2.getEndAt(), is(nullValue()));
        assertThat(taskRunDao.getTermAtOfTaskRun(taskRun2.getId()), is(notNullValue()));

        TaskAttemptProps attemptProps3 = taskRunDao.fetchLatestTaskAttempt(taskRun3.getId());
        assertThat(attemptProps3.getAttempt(), is(1));
        assertThat(attemptProps3.getStatus(), is(TaskRunStatus.UPSTREAM_FAILED));
        assertThat(attemptProps3.getLogPath(), is(nullValue()));
        assertThat(attemptProps3.getStartAt(), is(nullValue()));
        assertThat(attemptProps3.getEndAt(), is(nullValue()));
        assertThat(taskRunDao.getTermAtOfTaskRun(taskRun3.getId()), is(notNullValue()));
    }

    @Test
    public void retryTaskRunRecoverDownStream() {

        List<Task> taskList = MockTaskFactory.createTasksWithRelations(3, "0>>1;1>>2");

        long operatorId = taskList.get(0).getOperatorId();
        Operator op = MockOperatorFactory.createOperator()
                .cloneBuilder()
                .withId(operatorId)
                .withName("Operator_" + operatorId)
                .withClassName("testOperator")
                .withPackagePath(compileJar(NopOperator.class, NopOperator.class.getSimpleName()))
                .build();
        operatorDao.createWithId(op, operatorId);
        taskList.forEach(task -> taskDao.create(task));
        List<TaskRun> taskRunList = MockTaskRunFactory.createTaskRunsWithRelations(taskList, "0>>1");
        TaskRun taskRun1 = taskRunList.get(0);
        TaskRun taskRun2 = taskRunList.get(1);
        taskRunDao.createTaskRun(taskRun1);
        taskRunDao.createTaskRun(taskRun2);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                TaskAttempt taskAttempt = invocation.getArgument(0, TaskAttempt.class);
                if (taskAttempt.getTaskRun().getId().equals(taskRun1.getId())) {
                    taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.FAILED);
                    eventBus.post(prepareEvent(taskAttempt.getId()
                            , taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.FAILED));
                } else {
                    taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.SUCCESS);
                    eventBus.post(prepareEvent(taskAttempt.getId()
                            , taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.SUCCESS));
                }
                return null;
            }
        }).when(executor).submit(ArgumentMatchers.any());
        taskManager.submit(taskRunList);

        awaitUntilAttemptDone(taskRun1.getId() + 1);

        // verify update downStream
        TaskAttemptProps attemptProps1 = taskRunDao.fetchLatestTaskAttempt(taskRun1.getId());
        assertThat(attemptProps1.getAttempt(), is(1));
        assertThat(attemptProps1.getStatus(), is(TaskRunStatus.FAILED));
        assertThat(attemptProps1.getLogPath(), is(nullValue()));
        assertThat(attemptProps1.getStartAt(), is(nullValue()));
        assertThat(attemptProps1.getEndAt(), is(nullValue()));

        TaskAttemptProps attemptProps2 = taskRunDao.fetchLatestTaskAttempt(taskRun2.getId());
        assertThat(attemptProps2.getAttempt(), is(1));
        assertThat(attemptProps2.getStatus(), is(TaskRunStatus.UPSTREAM_FAILED));
        assertThat(attemptProps2.getLogPath(), is(nullValue()));
        assertThat(attemptProps2.getStartAt(), is(nullValue()));
        assertThat(attemptProps2.getEndAt(), is(nullValue()));
        assertThat(taskRunDao.getTermAtOfTaskRun(taskRun2.getId()), is(notNullValue()));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                TaskAttempt taskAttempt = invocation.getArgument(0, TaskAttempt.class);
                taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.SUCCESS);
                eventBus.post(prepareEvent(taskAttempt.getId()
                        , taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.SUCCESS));
                return null;
            }
        }).when(executor).submit(ArgumentMatchers.any());


        //retry taskRun1
        taskManager.retry(taskRunDao.fetchTaskRunById(taskRun1.getId()).get());


        // verify invoke downStream
        awaitUntilAttemptDone(taskRun2.getId() + 1);

        attemptProps2 = taskRunDao.fetchLatestTaskAttempt(taskRun2.getId());
        assertThat(attemptProps2.getId(), is(attemptProps2.getId()));
        assertThat(attemptProps2.getAttempt(), is(1));
        assertThat(attemptProps2.getStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(attemptProps2.getLogPath(), is(nullValue()));
        assertThat(attemptProps2.getStartAt(), is(nullValue()));
        assertThat(attemptProps2.getEndAt(), is(nullValue()));
        assertThat(taskRunDao.getTermAtOfTaskRun(taskRun2.getId()), is(notNullValue()));

    }

    @Test
    public void updateDownstreamShouldNotEffectOthersDependencies() {
        List<Task> taskList = MockTaskFactory.createTasksWithRelations(3, "0>>2;1>>2");
        List<TaskRun> taskRunList = MockTaskRunFactory.createTaskRunsWithRelations(taskList, "0>>2;1>>2");
        for (Task task : taskList) {
            taskDao.create(task);
        }
        TaskRun taskRun1 = taskRunList.get(0);
        TaskRun taskRun2 = taskRunList.get(1);
        TaskRun taskRun3 = taskRunList.get(2);
        taskRunDao.createTaskRun(taskRun1);
        taskRunDao.createTaskRun(taskRun2);
        taskRunDao.createTaskRun(taskRun3);

        //taskRun1 run failed
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                TaskAttempt taskAttempt = invocation.getArgument(0, TaskAttempt.class);
                if (taskAttempt.getTaskRun().getId().equals(taskRun1.getId())) {
                    taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.FAILED);
                    eventBus.post(prepareEvent(taskAttempt.getId()
                            , taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.FAILED));
                } else {
                    taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.SUCCESS);
                    eventBus.post(prepareEvent(taskAttempt.getId()
                            , taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.SUCCESS));
                }
                return null;
            }
        }).when(executor).submit(ArgumentMatchers.any());
        taskManager.submit(taskRunList);


        awaitUntilAttemptDone(taskRun1.getId() + 1);

        // verify update downStream
        TaskAttemptProps attemptProps1 = taskRunDao.fetchLatestTaskAttempt(taskRun1.getId());
        assertThat(attemptProps1.getAttempt(), is(1));
        assertThat(attemptProps1.getStatus(), is(TaskRunStatus.FAILED));
        assertThat(attemptProps1.getLogPath(), is(nullValue()));
        assertThat(attemptProps1.getStartAt(), is(nullValue()));
        assertThat(attemptProps1.getEndAt(), is(nullValue()));

        TaskAttemptProps attemptProps2 = taskRunDao.fetchLatestTaskAttempt(taskRun2.getId());
        assertThat(attemptProps2.getAttempt(), is(1));
        assertThat(attemptProps2.getStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(attemptProps2.getLogPath(), is(nullValue()));
        assertThat(attemptProps2.getStartAt(), is(nullValue()));
        assertThat(attemptProps2.getEndAt(), is(nullValue()));

        TaskAttemptProps attemptProps3 = taskRunDao.fetchLatestTaskAttempt(taskRun3.getId());
        assertThat(attemptProps3.getAttempt(), is(1));
        assertThat(attemptProps3.getStatus(), is(TaskRunStatus.UPSTREAM_FAILED));
        assertThat(attemptProps3.getLogPath(), is(nullValue()));
        assertThat(attemptProps3.getStartAt(), is(nullValue()));
        assertThat(attemptProps3.getEndAt(), is(nullValue()));

        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                TaskAttempt taskAttempt = invocation.getArgument(0, TaskAttempt.class);
                taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.SUCCESS);
                eventBus.post(prepareEvent(taskAttempt.getId()
                        , taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.SUCCESS));
                return null;
            }
        }).when(executor).submit(ArgumentMatchers.any());

        //retry taskRun1
        taskManager.retry(taskRunDao.fetchTaskRunById(taskRun1.getId()).get());

        // verify invoke downStream
        awaitUntilAttemptDone(taskRun3.getId() + 1);

        attemptProps3 = taskRunDao.fetchLatestTaskAttempt(taskRun3.getId());
        assertThat(attemptProps3.getId(), is(attemptProps3.getId()));
        assertThat(attemptProps3.getAttempt(), is(1));
        assertThat(attemptProps3.getStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(attemptProps3.getLogPath(), is(nullValue()));
        assertThat(attemptProps3.getStartAt(), is(nullValue()));
        assertThat(attemptProps3.getEndAt(), is(nullValue()));

    }

    @Test
    public void multiThreadSubmitShouldNotDuplicate() {
        Set<Long> submittedTaskRun = new HashSet<>();
        List<TaskRun> readyTaskRuns = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            Task task = MockTaskFactory.createTask();
            taskDao.create(task);
            TaskRun taskRun = MockTaskRunFactory.createTaskRun(task);
            taskRunDao.createTaskRun(taskRun);
            readyTaskRuns.add(taskRun);
        }
        ArgumentCaptor<TaskAttempt> captor = ArgumentCaptor.forClass(TaskAttempt.class);
        taskManager.submit(readyTaskRuns);
        doAnswer(new Answer() {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable {
                Thread.sleep(1000);
                TaskAttempt taskAttempt = invocation.getArgument(0, TaskAttempt.class);
                if (!submittedTaskRun.add(taskAttempt.getTaskRun().getId())) {
                    throw new IllegalStateException("taskAttemptId = " + taskAttempt.getId() + "is running");
                }
                taskRunDao.updateTaskAttemptStatus(taskAttempt.getId(), TaskRunStatus.SUCCESS);
                eventBus.post(prepareEvent(taskAttempt.getId()
                        , taskAttempt.getTaskName(), taskAttempt.getTaskId(), TaskRunStatus.RUNNING, TaskRunStatus.SUCCESS));
                return null;
            }
        }).when(executor).submit(ArgumentMatchers.any());
        for (int i = 0; i < 3; i++) {
            Thread thread = new Thread(() -> {
                    Task task = MockTaskFactory.createTask();
                    taskDao.create(task);
                    TaskRun taskRun = MockTaskRunFactory.createTaskRun(task);
                    taskRunDao.createTaskRun(taskRun);
                    taskManager.submit(Arrays.asList(taskRun));
            });
            thread.start();
        }
        await().atMost(60, TimeUnit.SECONDS).until(() ->
            submittedTaskRun.size() == 8
        );
        verify(executor, times(8)).submit(captor.capture());

    }


    @Test
    public void testCreateAttemptWhenTaskRunUpstreamFailedShouldUpstreamFailed(){
        Task task = MockTaskFactory.createTask();
        TaskRun taskRun = MockTaskRunFactory.
                createTaskRun(task).cloneBuilder().withStatus(TaskRunStatus.UPSTREAM_FAILED)
                .build();
        taskManager.submit(Arrays.asList(taskRun));

        //verify create attempt
        TaskAttemptProps created = taskRunDao.fetchLatestTaskAttempt(taskRun.getId());
        assertThat(created.getStatus(),is(TaskRunStatus.UPSTREAM_FAILED));
    }

    @Test
    public void testReSubmitTaskAttempt_AttemptTimesShouldInvariant(){
        Task task = MockTaskFactory.createTask();
        TaskRun taskRun = MockTaskRunFactory.createTaskRun(task);
        ArgumentCaptor<TaskAttempt> captor = ArgumentCaptor.forClass(TaskAttempt.class);
        taskDao.create(task);
        taskRunDao.createTaskRun(taskRun);
        taskManager.submit(Arrays.asList(taskRun));
        // verify first submit
        await().atMost(10, TimeUnit.SECONDS).until(this::invoked);
        verify(executor).submit(captor.capture());
        TaskAttempt taskAttempt = captor.getValue();
        assertThat(taskAttempt.getStatus(),is(TaskRunStatus.CREATED));
        assertThat(taskAttempt.getId(),is(taskRun.getId() + 1));
        assertThat(taskAttempt.getAttempt(),is(1));

        taskManager.submit(Arrays.asList(taskRun));
        // verify resubmit
        await().atMost(10, TimeUnit.SECONDS).until(this::invoked);
        TaskAttempt reSubmitAttempt = captor.getValue();
        assertThat(reSubmitAttempt.getStatus(),is(TaskRunStatus.CREATED));
        assertThat(reSubmitAttempt.getId(),is(taskRun.getId() + 1));
        assertThat(reSubmitAttempt.getAttempt(),is(1));
    }


    private TaskAttemptStatusChangeEvent prepareEvent(long taskAttemptId, String taskName, long taskId, TaskRunStatus from, TaskRunStatus to) {
        return new TaskAttemptStatusChangeEvent(taskAttemptId, from, to, taskName, taskId);
    }

    private String compileJar(Class<? extends KunOperator> operatorClass, String operatorClassName) {
        return OperatorCompiler.compileJar(operatorClass, operatorClassName);
    }

    private boolean invoked() {
        return !Mockito.mockingDetails(executor).getInvocations().isEmpty();
    }

    private List<TaskAttempt> getSubmittedTaskAttempts() {
        ArgumentCaptor<TaskAttempt> captor = ArgumentCaptor.forClass(TaskAttempt.class);
        verify(executor).submit(captor.capture());
        return captor.getAllValues();
    }

    private void awaitUntilAttemptDone(long attemptId) {
        await().atMost(120, TimeUnit.SECONDS).until(() -> {
            Optional<TaskRunStatus> s = taskRunDao.fetchTaskAttemptStatus(attemptId);
            return s.isPresent() && (s.get().isFinished());
        });
    }
}