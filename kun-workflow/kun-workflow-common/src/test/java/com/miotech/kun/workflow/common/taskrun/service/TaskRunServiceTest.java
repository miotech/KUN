package com.miotech.kun.workflow.common.taskrun.service;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.miotech.kun.workflow.LocalScheduler;
import com.miotech.kun.workflow.common.CommonTestBase;
import com.miotech.kun.workflow.common.exception.EntityNotFoundException;
import com.miotech.kun.workflow.common.resource.ResourceLoader;
import com.miotech.kun.workflow.common.task.dao.TaskDao;
import com.miotech.kun.workflow.common.taskrun.bo.TaskAttemptProps;
import com.miotech.kun.workflow.common.taskrun.dao.TaskRunDao;
import com.miotech.kun.workflow.common.taskrun.vo.TaskRunLogVO;
import com.miotech.kun.workflow.common.taskrun.vo.TaskRunVO;
import com.miotech.kun.workflow.core.Executor;
import com.miotech.kun.workflow.core.execution.Config;
import com.miotech.kun.workflow.core.model.common.Tick;
import com.miotech.kun.workflow.core.model.task.ScheduleConf;
import com.miotech.kun.workflow.core.model.task.ScheduleType;
import com.miotech.kun.workflow.core.model.task.Task;
import com.miotech.kun.workflow.core.model.taskrun.TaskAttempt;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import com.miotech.kun.workflow.core.model.taskrun.TaskRunStatus;
import com.miotech.kun.workflow.core.resource.Resource;
import com.miotech.kun.workflow.utils.DateTimeUtils;
import com.miotech.kun.workflow.utils.WorkflowIdGenerator;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;

public class TaskRunServiceTest extends CommonTestBase {
    private TaskRunService taskRunService;

    @Inject
    private TaskDao taskDao;

    @Inject
    private TaskRunDao taskRunDao;

    @Inject
    private ResourceLoader resourceLoader;

    private final Executor executor = mock(Executor.class);

    @Inject
    private LocalScheduler scheduler;

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();


    @Before
    public void setUp() {
        this.taskRunDao = spy(this.taskRunDao);
        this.taskRunService = spy(new TaskRunService(
                taskRunDao,
                resourceLoader,
                executor,
                scheduler
        ));
    }

    @Test
    public void testGetTaskRunDetail() {
        TaskRun taskRun = prepareData();
        TaskRunVO existedRun = taskRunService.getTaskRunDetail(taskRun.getId()).get();
        assertNotNull(existedRun);
        assertNotNull(existedRun.getTask());
    }


    private TaskRun prepareData() {
        long testId = 1L;

        Task task = Task.newBuilder().withId(22L)
                .withName("test task")
                .withDescription("")
                .withOperatorId(1L)
                .withScheduleConf(new ScheduleConf(ScheduleType.NONE, null))
                .withConfig(Config.EMPTY)
                .withDependencies(new ArrayList<>())
                .withTags(new ArrayList<>())
                .build();
        taskDao.create(task);

        TaskRun taskRun = TaskRun.newBuilder()
                .withId(testId)
                .withTask(task)
                .withStatus(TaskRunStatus.CREATED)
                .withConfig(Config.EMPTY)
                .withScheduleType(task.getScheduleConf().getType())
                .withDependentTaskRunIds(Collections.emptyList())
                .withInlets(Collections.emptyList())
                .withOutlets(Collections.emptyList())
                .withScheduledTick(new Tick(""))
                .build();
        taskRunDao.createTaskRun(taskRun);
        return taskRun;
    }

    @Test
    public void getTaskRunLog() throws IOException {
        Long taskRunId = 1L;
        String testStr = "hellow world";
        Resource resource = creatResource("xyz", testStr, 1);

        TaskAttemptProps attemptProps1 = TaskAttemptProps.newBuilder()
                .withAttempt(1)
                .withLogPath(resource.getLocation())
                .build();
        TaskAttemptProps attemptProps2 = TaskAttemptProps.newBuilder()
                .withAttempt(2)
                .withLogPath(resource.getLocation())
                .build();

        List<TaskAttemptProps> attempts = new ArrayList<>();
        attempts.add(attemptProps1);
        attempts.add(attemptProps2);

        Mockito.when(taskRunDao.fetchAttemptsPropByTaskRunId(taskRunId))
                .thenReturn(attempts);

        TaskRunLogVO taskRunLogVO;

        // attempt = 1
        taskRunLogVO = taskRunService.getTaskRunLog(taskRunId, 1, 0, 3);
        assertEquals(1, taskRunLogVO.getLogs().size());
        assertEquals(1, taskRunLogVO.getAttempt());

        // attempt = 2
        taskRunLogVO = taskRunService.getTaskRunLog(taskRunId, 2, 0, 3);
        assertEquals(1, taskRunLogVO.getLogs().size());
        assertEquals(2, taskRunLogVO.getAttempt());

        // attempt = -1 as latest
        taskRunLogVO = taskRunService.getTaskRunLog(taskRunId,-1,0, 3);
        assertEquals(1, taskRunLogVO.getLogs().size());
        assertEquals(2, taskRunLogVO.getAttempt());
        assertEquals(testStr, taskRunLogVO.getLogs().get(0));

    }


    @Test
    public void getTaskRunLog_withstartLine() throws IOException {
        Long taskRunId = 1L;
        Resource resource = creatResource("xyz", "hello world\n", 3);

        TaskAttemptProps attemptProps = TaskAttemptProps.newBuilder()
                .withAttempt(1)
                .withLogPath(resource.getLocation())
                .build();

        List<TaskAttemptProps> attempts = new ArrayList<>();
        attempts.add(attemptProps);

        Mockito.when(taskRunDao.fetchAttemptsPropByTaskRunId(taskRunId))
                .thenReturn(attempts);

        TaskRunLogVO taskRunLogVO;

        taskRunLogVO = taskRunService.getTaskRunLog(taskRunId, 1, 0, 3);
        assertEquals(3, taskRunLogVO.getLogs().size());

        taskRunLogVO = taskRunService.getTaskRunLog(taskRunId, 1, 2, 3);
        assertEquals(1, taskRunLogVO.getLogs().size());

        taskRunLogVO = taskRunService.getTaskRunLog(taskRunId, 1, 3, 3);
        assertEquals(0, taskRunLogVO.getLogs().size());
    }

    @Test
    public void getTaskRunLog_withendLine() throws IOException {
        Long taskRunId = 1L;
        Resource resource = creatResource("xyz", "hello world\n", 3);


        TaskAttemptProps attemptProps = TaskAttemptProps.newBuilder()
                .withAttempt(1)
                .withLogPath(resource.getLocation())
                .build();

        List<TaskAttemptProps> attempts = new ArrayList<>();
        attempts.add(attemptProps);

        Mockito.when(taskRunDao.fetchAttemptsPropByTaskRunId(taskRunId))
                .thenReturn(attempts);

        TaskRunLogVO taskRunLogVO;

        taskRunLogVO = taskRunService.getTaskRunLog(taskRunId, 1, 0, 1);
        // should return line with index 0 (line 1)
        assertEquals(1, taskRunLogVO.getLogs().size());

        // should return line with index 0, 1, 2, 3 (line 1, 2, 3, 4)
        taskRunLogVO = taskRunService.getTaskRunLog(taskRunId, 1, 0, 4);
        assertEquals(3, taskRunLogVO.getLogs().size());
    }

    @Test
    public void getTaskRunLog_withNotExistingTaskRun_shouldThrowException() throws IOException {
        Long taskRunId = 1L;

        Mockito.when(taskRunDao.fetchAttemptsPropByTaskRunId(taskRunId))
                .thenReturn(Collections.emptyList());
        try {
            taskRunService.getTaskRunLog(taskRunId,-1, 1, 2);
        } catch (Exception e) {
            assertEquals(EntityNotFoundException.class, e.getClass());
            assertEquals("Cannot find task attempt -1 of task run with id = 1.", e.getMessage());
        }
    }

    @Test
    public void getTaskRunLog_shouldAllowSearchByNegativeIndex() throws IOException {
        // Prepare
        Long taskRunId = 1L;
        String testStr = "line 1\nline 2\nline 3\nline 4\n";
        Resource resource = creatResource("xyz", testStr, 1);
        TaskAttemptProps attemptProps = TaskAttemptProps.newBuilder()
                .withAttempt(1)
                .withLogPath(resource.getLocation())
                .build();

        Mockito.when(taskRunDao.fetchAttemptsPropByTaskRunId(taskRunId))
                .thenReturn(Collections.singletonList(attemptProps));

        TaskRunLogVO vo = taskRunService.getTaskRunLog(taskRunId,-1, -3, null);
        assertThat(vo.getLogs().size(), is(3));
        assertThat(vo.getLogs().get(0), is("line 2"));
        assertThat(vo.getAttempt(), is(1));
        assertThat(vo.getStartLine(), is(1));
        assertThat(vo.getEndLine(), is(3));
    }

    @Test
    public void abortTaskRun_shouldSendAbortSignalsToExecutors() {
        // Prepare
        Mockito.doAnswer(invocation -> {
            TaskAttemptProps attemptPropsTemplate = TaskAttemptProps.newBuilder()
                    .withStatus(TaskRunStatus.RUNNING)
                    .build();
            return attemptPropsTemplate.cloneBuilder().withId(11L).build();
        }).when(taskRunDao).fetchLatestTaskAttempt(Mockito.anyLong());

        Map<Long, Boolean> stopSignalReceived = new HashMap<>();
        Mockito.doAnswer(invocation -> {
            Long id = invocation.getArgument(0);
            stopSignalReceived.put(id, true);
            return true;
        }).when(executor).cancel(Mockito.anyLong());

        // process
        boolean result = taskRunService.abortTaskRun(1L);

        // Validate
        assertThat(result, is(true));
        assertThat(stopSignalReceived.getOrDefault(11L, false), is(true));
    }

    private Resource creatResource(String fileName, String text, int times) throws IOException {
        File file = tempFolder.newFile(fileName);
        Resource resource = resourceLoader.getResource("file://" + file.getPath(), true);
        Writer writer= new PrintWriter(resource.getOutputStream());

        for (int i=0; i < times; i++) {
            writer.write(text);
        }
        writer.flush();
        return resource;
    }

    @Test
    public void fetchLatestTaskRuns_shouldFetchLatestTaskRunsAndReturnsMappedResults() {
        // Prepare
        prepareDataForFetchLatestTaskRuns();
        long task1Id = 22L;
        long task2Id = 33L;
        long task3Id = 44L;

        // Process
        List<Long> taskIdsToQuery = Lists.newArrayList(task1Id, task2Id, task3Id);
        Map<Long, List<TaskRunVO>> mappings = taskRunService.fetchLatestTaskRuns(taskIdsToQuery, 10);

        // Validate
        assertThat(mappings.size(), is(3));
        // task1 has 20 runs, but size of fetched result should be limited to 10
        assertThat(mappings.get(task1Id).size(), is(10));
        assertThat(mappings.get(task2Id).size(), is(5));
        assertThat(mappings.get(task3Id).size(), is(0));
    }


    @Test
    public void fetchLatestTaskRunsOrderByCreateTime(){
        prepareTaskRuns();
        List<Long> taskIdsToQuery = Lists.newArrayList(22L);
        Map<Long, List<TaskRunVO>> mappings = taskRunService.fetchLatestTaskRuns(taskIdsToQuery, 10);
        List<TaskRunVO> taskRunVOS = mappings.get(22L);
        assertThat(taskRunVOS,hasSize(2));
        assertThat(taskRunVOS.get(0).getId(),is(2L));
        assertThat(taskRunVOS.get(1).getId(),is(1L));
    }


    private void prepareTaskRuns(){
        Task task1 = Task.newBuilder().withId(22L)
                .withName("test task 1")
                .withDescription("")
                .withOperatorId(1L)
                .withScheduleConf(new ScheduleConf(ScheduleType.NONE, null))
                .withConfig(Config.EMPTY)
                .withDependencies(new ArrayList<>())
                .withTags(new ArrayList<>())
                .build();

        taskDao.create(task1);
        TaskRun taskRun1 = TaskRun.newBuilder()
                .withId(1L)
                .withTask(task1)
                .withStatus(TaskRunStatus.INITIALIZING)
                .withConfig(Config.EMPTY)
                .withScheduleType(task1.getScheduleConf().getType())
                .withDependentTaskRunIds(Collections.emptyList())
                .withInlets(Collections.emptyList())
                .withOutlets(Collections.emptyList())
                .withScheduledTick(new Tick(DateTimeUtils.now()))
                .build();
        taskRunDao.createTaskRun(taskRun1);
        try {
            Thread.sleep(1000l);
        }catch (Exception e){

        }


        TaskRun taskRun2 = TaskRun.newBuilder()
                .withId(2L)
                .withTask(task1)
                .withStatus(TaskRunStatus.RUNNING)
                .withConfig(Config.EMPTY)
                .withScheduleType(task1.getScheduleConf().getType())
                .withDependentTaskRunIds(Collections.emptyList())
                .withInlets(Collections.emptyList())
                .withOutlets(Collections.emptyList())
                .withScheduledTick(new Tick(DateTimeUtils.now().plusMinutes(-1)))
                .withStartAt(DateTimeUtils.now().plusMinutes(-1))
                .build();
        taskRunDao.createTaskRun(taskRun2);
    }

    private void prepareDataForFetchLatestTaskRuns() {
        Task task1 = Task.newBuilder().withId(22L)
                .withName("test task 1")
                .withDescription("")
                .withOperatorId(1L)
                .withScheduleConf(new ScheduleConf(ScheduleType.NONE, null))
                .withConfig(Config.EMPTY)
                .withDependencies(new ArrayList<>())
                .withTags(new ArrayList<>())
                .build();
        Task task2 = task1.cloneBuilder().withId(33L).withName("test task 2").build();

        taskDao.create(task1);
        taskDao.create(task2);

        // insert 20 runs for task 1
        for (int i = 1; i <= 20; i++) {
            TaskRun taskRun = TaskRun.newBuilder()
                    .withId(100L + i)
                    .withTask(task1)
                    .withStatus(TaskRunStatus.RUNNING)
                    .withConfig(Config.EMPTY)
                    .withScheduleType(task1.getScheduleConf().getType())
                    .withDependentTaskRunIds(Collections.emptyList())
                    .withInlets(Collections.emptyList())
                    .withOutlets(Collections.emptyList())
                    .withScheduledTick(new Tick(""))
                    .build();
            taskRunDao.createTaskRun(taskRun);
        }
        // insert 5 runs for task 2
        for (int i = 1; i <= 5; i++) {
            TaskRun taskRun = TaskRun.newBuilder()
                    .withId(120L + i)
                    .withTask(task2)
                    .withStatus(TaskRunStatus.RUNNING)
                    .withConfig(Config.EMPTY)
                    .withScheduleType(task2.getScheduleConf().getType())
                    .withDependentTaskRunIds(Collections.emptyList())
                    .withInlets(Collections.emptyList())
                    .withOutlets(Collections.emptyList())
                    .withScheduledTick(new Tick(""))
                    .build();
            taskRunDao.createTaskRun(taskRun);
        }
    }

    @Test
    public void rerunTaskRun_whenLatestAttemptIsFinished_shouldSubmitNewAttemptToExecutor() {
        // 1. Prepare
        TaskRun taskRunFailed = prepareTaskRunAndTaskAttempt(TaskRunStatus.FAILED);
        mockExecutorOnSubmit();

        // 2. Process
        boolean rerunSuccessfully = taskRunService.rerunTaskRun(taskRunFailed.getId());

        // 3. Validate
        assertTrue(rerunSuccessfully);
    }

    @Test
    public void rerunTaskRun_whenLatestAttemptNotFinished_shouldThrowsIllegalStateException() throws IllegalStateException {
        // 1. Prepare
        TaskRun taskRunFailed = prepareTaskRunAndTaskAttempt(TaskRunStatus.RUNNING);
        mockExecutorOnSubmit();

        // 2. Validate
        expectedEx.expect(IllegalStateException.class);
        expectedEx.expectMessage("taskRun status must be finished");

        // 3. Process
        taskRunService.rerunTaskRun(taskRunFailed.getId());

    }

    @Test
    public void rerunTaskRun_whenTaskRunDoesNotExist_shouldProduceNoEffect() {
        // 1. Prepare
        mockExecutorOnSubmit();

        // 2. Process
        boolean rerunSuccessfully = taskRunService.rerunTaskRun(1234567L);

        // 3. Validate
        assertFalse(rerunSuccessfully);
    }

    @Test
    public void rerunTaskRun_whenLatestAttemptNotFound_shouldThrowIllegalStateException() {
        // 1. Prepare a task run but does not create any task run
        TaskRun preparedTaskRunWithoutAttempt = prepareData();

        try {
            taskRunService.rerunTaskRun(preparedTaskRunWithoutAttempt.getId());
            fail();
        } catch (Exception e) {
            assertThat(e, instanceOf(IllegalStateException.class));
        }
    }

    @Ignore
    @Test
    public void rerunTaskRun_whenConcurrentlyInvoked_shouldOnlyTakeOneEffect() throws Exception {
        // 1. Prepare
        TaskRun taskRunFailed = prepareTaskRunAndTaskAttempt(TaskRunStatus.FAILED);
        mockExecutorOnSubmit();

        List<RunnableFuture> futures = new ArrayList<>();
        for (int i = 0; i < 5; ++i) {
            futures.add(new FutureTask(() -> {
                return taskRunService.rerunTaskRun(taskRunFailed.getId());
            }));
        }

        // 2. Process
        ExecutorService es = Executors.newCachedThreadPool();
        futures.forEach(future -> {
            es.execute(future);
        });
        int successCount = 0;
        for (int i = 0; i < 5; ++i) {
            Boolean rerunSuccessFlag = false;
            try {
                rerunSuccessFlag = (Boolean) futures.get(i).get();
            } catch (Exception e) {
                // do nothing
            }

            if (rerunSuccessFlag) {
                successCount += 1;
            }
        }

        // 3. Validate
        assertThat(successCount, is(1));
    }

    private TaskRun prepareTaskRunAndTaskAttempt(TaskRunStatus runStatus) {
        long taskId = WorkflowIdGenerator.nextTaskId();
        long taskRunId = WorkflowIdGenerator.nextTaskRunId();
        long taskAttemptId = WorkflowIdGenerator.nextTaskAttemptId(taskRunId, 1);

        Task task = Task.newBuilder().withId(taskId)
                .withName("test task")
                .withDescription("")
                .withOperatorId(1L)
                .withScheduleConf(new ScheduleConf(ScheduleType.NONE, null))
                .withConfig(Config.EMPTY)
                .withDependencies(new ArrayList<>())
                .withTags(new ArrayList<>())
                .build();
        taskDao.create(task);

        TaskRun taskRun = TaskRun.newBuilder()
                .withId(taskRunId)
                .withTask(task)
                .withStatus(runStatus)
                .withConfig(Config.EMPTY)
                .withScheduleType(task.getScheduleConf().getType())
                .withDependentTaskRunIds(Collections.emptyList())
                .withInlets(Collections.emptyList())
                .withOutlets(Collections.emptyList())
                .withScheduledTick(new Tick(""))
                .build();
        taskRunDao.createTaskRun(taskRun);
        taskRunDao.createAttempt(TaskAttempt.newBuilder()
                .withId(taskAttemptId)
                .withTaskRun(taskRun)
                .withAttempt(1)
                .withStatus(runStatus)
                .build()
        );
        return taskRun;
    }

    private void mockExecutorOnSubmit() {
        Mockito.doAnswer(invocation -> {
            TaskAttempt attemptArgument = invocation.getArgument(0);
            TaskAttemptProps persistedLatestAttempt = taskRunDao.fetchLatestTaskAttempt(attemptArgument.getTaskRun().getId());
            return persistedLatestAttempt.getId().equals(attemptArgument.getId());
        }).when(executor).submit(Mockito.isA(TaskAttempt.class));
    }

    @Test
    public void getLineCountOfFile_shouldBeAbleToReadEntireLogFile() throws IOException {
        InputStream mockLogResourceStream = getClass().getResourceAsStream("/testcase/example.log");

        BufferedReader reader = new BufferedReader(new InputStreamReader(mockLogResourceStream));
        Triple<List<String>, Integer, Integer> result = taskRunService.readLinesFromLogFile(reader, 30, 0, null);
        assertThat(result.getLeft().size(), is(30));
        assertThat(result.getLeft().get(0), is("This is line 1."));
        assertThat(result.getLeft().get(29), is("This is line 30."));
        assertThat(result.getMiddle(), is(0));
        assertThat(result.getRight(), is(29));
    }

    @Test
    public void getLineCountOfFile_withRange_shouldBeAbleToRead() throws IOException {
        InputStream mockLogResourceStream = getClass().getResourceAsStream("/testcase/example.log");

        BufferedReader reader = new BufferedReader(new InputStreamReader(mockLogResourceStream));
        Triple<List<String>, Integer, Integer> result = taskRunService.readLinesFromLogFile(reader, 30, 1, 4);
        // Should read line 2, line 3, line 4
        assertThat(result.getLeft().size(), is(3));
        assertThat(result.getLeft().get(0), is("This is line 2."));
        assertThat(result.getLeft().get(2), is("This is line 4."));
        assertThat(result.getMiddle(), is(1));
        assertThat(result.getRight(), is(3));
    }

    @Test
    public void getLineCountOfFile_withNegativeIndexes_shouldBeAbleToRead() throws IOException {
        InputStream mockLogResourceStream = getClass().getResourceAsStream("/testcase/example.log");

        BufferedReader reader = new BufferedReader(new InputStreamReader(mockLogResourceStream));
        Triple<List<String>, Integer, Integer> result = taskRunService.readLinesFromLogFile(reader, 30, 1, -1);
        assertThat(result.getLeft().size(), is(28));
        assertThat(result.getLeft().get(0), is("This is line 2."));
        assertThat(result.getLeft().get(27), is("This is line 29."));
        assertThat(result.getMiddle(), is(1));
        assertThat(result.getRight(), is(28));
    }

    @Test
    public void getLineCountOfFile_withNegativeIndexes_shouldBeAbleToReadLast5Lines() throws IOException {
        InputStream mockLogResourceStream = getClass().getResourceAsStream("/testcase/example.log");

        BufferedReader reader = new BufferedReader(new InputStreamReader(mockLogResourceStream));
        Triple<List<String>, Integer, Integer> result = taskRunService.readLinesFromLogFile(reader, 30, -5, null);
        assertThat(result.getLeft().size(), is(5));
        assertThat(result.getLeft().get(0), is("This is line 26."));
        assertThat(result.getLeft().get(4), is("This is line 30."));
        assertThat(result.getMiddle(), is(25));
        assertThat(result.getRight(), is(29));
    }
}
