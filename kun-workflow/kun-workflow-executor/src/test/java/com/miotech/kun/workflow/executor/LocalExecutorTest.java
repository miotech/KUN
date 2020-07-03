package com.miotech.kun.workflow.executor;

import com.google.common.collect.Iterables;
import com.google.common.eventbus.EventBus;
import com.google.common.io.ByteStreams;
import com.miotech.kun.commons.testing.DatabaseTestBase;
import com.miotech.kun.commons.utils.ExceptionUtils;
import com.miotech.kun.workflow.common.operator.dao.OperatorDao;
import com.miotech.kun.workflow.common.resource.ResourceLoader;
import com.miotech.kun.workflow.common.task.dao.TaskDao;
import com.miotech.kun.workflow.common.taskrun.bo.TaskAttemptProps;
import com.miotech.kun.workflow.common.taskrun.dao.TaskRunDao;
import com.miotech.kun.workflow.core.Executor;
import com.miotech.kun.workflow.core.event.Event;
import com.miotech.kun.workflow.core.event.TaskAttemptFinishedEvent;
import com.miotech.kun.workflow.core.event.TaskAttemptStatusChangeEvent;
import com.miotech.kun.workflow.core.model.lineage.DataStore;
import com.miotech.kun.workflow.core.model.taskrun.TaskAttempt;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import com.miotech.kun.workflow.core.model.taskrun.TaskRunStatus;
import com.miotech.kun.workflow.core.resource.Resource;
import com.miotech.kun.workflow.executor.local.LocalExecutor;
import com.miotech.kun.workflow.testing.event.EventCollector;
import com.miotech.kun.workflow.testing.factory.MockOperatorFactory;
import com.miotech.kun.workflow.testing.factory.MockTaskAttemptFactory;
import com.miotech.kun.workflow.utils.ResourceUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.inject.Inject;
import java.io.*;
import java.net.URL;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.shazam.shazamcrest.matcher.Matchers.sameBeanAs;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;

public class LocalExecutorTest extends DatabaseTestBase {
    private final static String TEST_OPERATOR1 = "TestOperator1";
    private final static String TEST_OPERATOR2 = "TestOperator2";
    private final static String TEST_OPERATOR3 = "TestOperator3";

    @Inject
    private Executor executor;

    @Inject
    private OperatorDao operatorDao;

    @Inject
    private TaskRunDao taskRunDao;

    @Inject
    private TaskDao taskDao;

    @Inject
    private ResourceLoader resourceLoader;

    @Inject
    private EventBus eventBus;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private EventCollector eventCollector;

    @Override
    protected void configuration() {
        super.configuration();
        bind(EventBus.class, new EventBus());
        bind(Executor.class, LocalExecutor.class);
    }

    @Before
    @Override
    public void setUp() {
        super.setUp();
        eventCollector = new EventCollector();
        eventBus.register(eventCollector);
    }

    @Test
    public void testSubmit_ok() throws IOException {
        // prepare
        TaskAttempt attempt = prepareAttempt(TEST_OPERATOR1);

        // process
        executor.submit(attempt);

        // verify
        awaitUntilAttemptDone(attempt.getId());

        // task_run and task_attempt
        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(attempt.getTaskRun().getId());
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(attemptProps.getLogPath(), is(notNullValue()));
        assertThat(attemptProps.getStartAt(), is(notNullValue()));
        assertThat(attemptProps.getEndAt(), is(notNullValue()));

        TaskRun taskRun = taskRunDao.fetchLatestTaskRun(attempt.getTaskRun().getTask().getId());
        assertThat(taskRun.getStatus(), is(attemptProps.getStatus()));
        assertThat(taskRun.getStartAt(), is(attemptProps.getStartAt()));
        assertThat(taskRun.getEndAt(), is(attemptProps.getEndAt()));

        // logs
        Resource log = resourceLoader.getResource(attemptProps.getLogPath());
        List<String> content = ResourceUtils.lines(log.getInputStream()).collect(Collectors.toList());
        assertThat(content.size(), is(2));
        assertThat(content.get(0), containsString("Hello, world!"));
        assertThat(content.get(1), containsString("ContextClassLoader: java.net.FactoryURLClassLoader"));

        // events
        assertStatusProgress(attempt.getId(),
                TaskRunStatus.CREATED,
                TaskRunStatus.QUEUED,
                TaskRunStatus.RUNNING,
                TaskRunStatus.SUCCESS);

        TaskAttemptFinishedEvent finishedEvent = getFinishedEvent(attempt.getId());
        assertThat(finishedEvent.getAttemptId(), is(attempt.getId()));
        assertThat(finishedEvent.getFinalStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(finishedEvent.getInlets(), hasSize(2));
        assertThat(finishedEvent.getOutlets(), hasSize(1));

        // inlets/outlets
        List<DataStore> inlets = taskRun.getInlets();
        List<DataStore> outlets = taskRun.getOutlets();
        assertThat(finishedEvent.getInlets(), sameBeanAs(inlets));
        assertThat(finishedEvent.getOutlets(), sameBeanAs(outlets));
    }


    @Test
    public void testSubmit_then_overwriteOperatorJar() throws IOException {
        // prepare
        TaskAttempt attempt = prepareAttempt(TEST_OPERATOR1);

        // process
        executor.submit(attempt);
        awaitUntilAttemptDone(attempt.getId());
        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(attempt.getTaskRun().getId());
        Resource log = resourceLoader.getResource(attemptProps.getLogPath());
        List<String> content = ResourceUtils.lines(log.getInputStream()).collect(Collectors.toList());
        assertThat(content.get(0), containsString("Hello, world!"));

        // overwrite operator jar
        attempt = prepareAttempt(TEST_OPERATOR1);
        overwriteTestJarFile();
        executor.submit(attempt);
        awaitUntilAttemptDone(attempt.getId());

        // task_run and task_attempt
        attemptProps = taskRunDao.fetchLatestTaskAttempt(attempt.getTaskRun().getId());

        // logs
        log = resourceLoader.getResource(attemptProps.getLogPath());
        content = ResourceUtils.lines(log.getInputStream()).collect(Collectors.toList());
        assertThat(content.get(0), containsString("Hello, world2!"));
    }

    @Test
    public void testSubmit_ok_concurrent_running() throws IOException {
        // prepare
        TaskAttempt attempt1 = prepareAttempt(TEST_OPERATOR1);
        TaskAttempt attempt2 = prepareAttempt(TEST_OPERATOR2);

        // process
        executor.submit(attempt1);
        executor.submit(attempt2);

        // verify
        awaitUntilAttemptDone(attempt1.getId());
        awaitUntilAttemptDone(attempt2.getId());

        // task_run and task_attempt
        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(attempt1.getTaskRun().getId());
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(attemptProps.getLogPath(), is(notNullValue()));
        assertThat(attemptProps.getStartAt(), is(notNullValue()));
        assertThat(attemptProps.getEndAt(), is(notNullValue()));

        attemptProps = taskRunDao.fetchLatestTaskAttempt(attempt2.getTaskRun().getId());
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.FAILED));
        assertThat(attemptProps.getLogPath(), is(notNullValue()));
        assertThat(attemptProps.getStartAt(), is(notNullValue()));
        assertThat(attemptProps.getEndAt(), is(notNullValue()));

        // events
        assertStatusProgress(attempt1.getId(),
                TaskRunStatus.CREATED,
                TaskRunStatus.QUEUED,
                TaskRunStatus.RUNNING,
                TaskRunStatus.SUCCESS);

        TaskAttemptFinishedEvent finishedEvent = getFinishedEvent(attempt1.getId());
        assertThat(finishedEvent.getAttemptId(), is(attempt1.getId()));
        assertThat(finishedEvent.getFinalStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(finishedEvent.getInlets(), hasSize(2));
        assertThat(finishedEvent.getOutlets(), hasSize(1));

        assertStatusProgress(attempt2.getId(),
                TaskRunStatus.CREATED,
                TaskRunStatus.QUEUED,
                TaskRunStatus.RUNNING,
                TaskRunStatus.FAILED);

        finishedEvent = getFinishedEvent(attempt2.getId());
        assertThat(finishedEvent.getAttemptId(), is(attempt2.getId()));
        assertThat(finishedEvent.getFinalStatus(), is(TaskRunStatus.FAILED));
        assertThat(finishedEvent.getInlets(), hasSize(0));
        assertThat(finishedEvent.getOutlets(), hasSize(0));
    }

    @Test
    public void testSubmit_fail_running_failure() throws IOException {
        // prepare
        TaskAttempt attempt = prepareAttempt(TEST_OPERATOR2);

        // process
        executor.submit(attempt);

        // verify
        awaitUntilAttemptDone(attempt.getId());

        // task_run and task_attempt
        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(attempt.getTaskRun().getId());
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.FAILED));
        assertThat(attemptProps.getLogPath(), is(notNullValue()));
        assertThat(attemptProps.getStartAt(), is(notNullValue()));
        assertThat(attemptProps.getEndAt(), is(notNullValue()));

        TaskRun taskRun = taskRunDao.fetchLatestTaskRun(attempt.getTaskRun().getTask().getId());
        assertThat(taskRun.getStatus(), is(attemptProps.getStatus()));
        assertThat(taskRun.getStartAt(), is(attemptProps.getStartAt()));
        assertThat(taskRun.getEndAt(), is(attemptProps.getEndAt()));

        // logs
        Resource log = resourceLoader.getResource(attemptProps.getLogPath());
        List<String> content = ResourceUtils.lines(log.getInputStream()).collect(Collectors.toList());
        assertThat(content.size(), is(1));
        assertThat(content.get(0), containsString("Execution Failed"));

        // events
        assertStatusProgress(attempt.getId(),
                TaskRunStatus.CREATED,
                TaskRunStatus.QUEUED,
                TaskRunStatus.RUNNING,
                TaskRunStatus.FAILED);
    }

    @Test
    public void testSubmit_fail_unexpected_exception() throws IOException {
        // prepare
        TaskAttempt attempt = prepareAttempt(TEST_OPERATOR3);

        // process
        executor.submit(attempt);

        // verify
        awaitUntilAttemptDone(attempt.getId());

        // task_run and task_attempt
        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(attempt.getTaskRun().getId());
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.FAILED));
        assertThat(attemptProps.getLogPath(), is(notNullValue()));
        assertThat(attemptProps.getStartAt(), is(notNullValue()));
        assertThat(attemptProps.getEndAt(), is(notNullValue()));

        TaskRun taskRun = taskRunDao.fetchLatestTaskRun(attempt.getTaskRun().getTask().getId());
        assertThat(taskRun.getStatus(), is(attemptProps.getStatus()));
        assertThat(taskRun.getStartAt(), is(attemptProps.getStartAt()));
        assertThat(taskRun.getEndAt(), is(attemptProps.getEndAt()));

        // logs
        Resource log = resourceLoader.getResource(attemptProps.getLogPath());
        List<String> content = ResourceUtils.lines(log.getInputStream()).collect(Collectors.toList());
        assertThat(content.get(0), containsString("Unexpected exception occurred"));

        // events
        assertStatusProgress(attempt.getId(),
                TaskRunStatus.CREATED,
                TaskRunStatus.QUEUED,
                TaskRunStatus.RUNNING,
                TaskRunStatus.FAILED);
    }

    @Test
    public void testSubmit_fail_operator_not_found() throws IOException {
        // prepare
        TaskAttempt attempt = prepareAttempt("TestOperator999");

        // process
        executor.submit(attempt);

        // verify
        awaitUntilAttemptDone(attempt.getId());

        // task_run and task_attempt
        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(attempt.getTaskRun().getId());
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.FAILED));
        assertThat(attemptProps.getLogPath(), is(notNullValue()));
        assertThat(attemptProps.getStartAt(), is(notNullValue()));
        assertThat(attemptProps.getEndAt(), is(notNullValue()));

        TaskRun taskRun = taskRunDao.fetchLatestTaskRun(attempt.getTaskRun().getTask().getId());
        assertThat(taskRun.getStatus(), is(attemptProps.getStatus()));
        assertThat(taskRun.getStartAt(), is(attemptProps.getStartAt()));
        assertThat(taskRun.getEndAt(), is(attemptProps.getEndAt()));

        // logs
        Resource log = resourceLoader.getResource(attemptProps.getLogPath());
        List<String> content = ResourceUtils.lines(log.getInputStream()).collect(Collectors.toList());
        assertThat(content.get(1), containsString("Failed to load jar"));

        // events
        assertStatusProgress(attempt.getId(),
                TaskRunStatus.CREATED,
                TaskRunStatus.QUEUED,
                TaskRunStatus.RUNNING,
                TaskRunStatus.FAILED);
    }

    private TaskAttempt prepareAttempt(String operatorClassName) {
        TaskAttempt attempt = MockTaskAttemptFactory.createTaskAttempt();

        long operatorId = attempt.getTaskRun().getTask().getOperatorId();
        com.miotech.kun.workflow.core.model.operator.Operator
                op = MockOperatorFactory.createOperator()
                .cloneBuilder()
                .withId(operatorId)
                .withName("Operator_" + operatorId)
                .withClassName(operatorClassName)
                .withPackagePath(findTestJarFile())
                .build();
        operatorDao.createWithId(op, operatorId);
        taskDao.create(attempt.getTaskRun().getTask());
        taskRunDao.createTaskRun(attempt.getTaskRun());
        taskRunDao.createAttempt(attempt);

        return attempt;
    }

    private void assertStatusProgress(Long attemptId, TaskRunStatus... asserts) {
        checkArgument(asserts.length > 1);

        List<Event> events = eventCollector.getEvents();

        List<Event> eventsOfAttempt = events.stream()
                .filter(e -> e instanceof TaskAttemptStatusChangeEvent &&
                        ((TaskAttemptStatusChangeEvent) e).getAttemptId() == attemptId)
                .collect(Collectors.toList());

        for (int i = 0; i < asserts.length - 1; i++) {
            TaskAttemptStatusChangeEvent event = (TaskAttemptStatusChangeEvent) eventsOfAttempt.get(i);
            assertThat(event.getAttemptId(), is(attemptId));
            assertThat(event.getFromStatus(), is(asserts[i]));
            assertThat(event.getToStatus(), is(asserts[i + 1]));
        }
    }

    private TaskAttemptFinishedEvent getFinishedEvent(Long attemptId) {
        List<Event> events = eventCollector.getEvents().stream()
                .filter(e -> e instanceof TaskAttemptFinishedEvent &&
                        ((TaskAttemptFinishedEvent) e).getAttemptId() == attemptId)
                .collect(Collectors.toList());
        return (TaskAttemptFinishedEvent) Iterables.getOnlyElement(events);
    }

    private String findTestJarFile() {
        String fileName = "testOperators.jar";
        URL url = getClass().getClassLoader().getResource(fileName);
        File file = createTempFile(fileName, url);
        return "file:" + file.getPath();
    }

    private String overwriteTestJarFile() {
        String fileName = "testOperators2.jar";
        String targetFileName = "testOperators.jar";
        URL url = getClass().getClassLoader().getResource(fileName);
        File file = createTempFile(targetFileName, url);
        return "file:" + file.getPath();
    }

    private File createTempFile(String fileName, URL url) {
        File file = new File(tempFolder.getRoot().getPath() + "/"  + fileName);
        if (file.exists()) {
            file.delete();
        }
        Resource outputResource = resourceLoader.getResource("file://" + file.getPath(),
                true);
        Resource inputResource = resourceLoader.getResource(url.toString(),
                false);
        try {
            ByteStreams.copy(inputResource.getInputStream(), outputResource.getOutputStream());
        } catch (IOException e) {
            throw ExceptionUtils.wrapIfChecked(e);
        }
        return file;
    }

    private void awaitUntilAttemptDone(long attemptId) {
        await().atMost(10, TimeUnit.SECONDS).until(() -> {
            Optional<TaskRunStatus> s = taskRunDao.fetchTaskAttemptStatus(attemptId);
            return s.isPresent() && (s.get().isSuccess() || s.get().isFailure());
        });
    }
}