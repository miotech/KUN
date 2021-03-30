package com.miotech.kun.workflow.executor.kubernetes;

import com.google.common.eventbus.EventBus;
import com.google.common.util.concurrent.Uninterruptibles;
import com.google.inject.Inject;
import com.miotech.kun.commons.utils.Props;
import com.miotech.kun.workflow.common.operator.dao.OperatorDao;
import com.miotech.kun.workflow.common.task.dao.TaskDao;
import com.miotech.kun.workflow.common.taskrun.bo.TaskAttemptProps;
import com.miotech.kun.workflow.common.taskrun.dao.TaskRunDao;
import com.miotech.kun.workflow.core.event.Event;
import com.miotech.kun.workflow.core.event.TaskAttemptStatusChangeEvent;
import com.miotech.kun.workflow.core.model.operator.Operator;
import com.miotech.kun.workflow.core.model.taskrun.TaskAttempt;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import com.miotech.kun.workflow.core.model.taskrun.TaskRunStatus;
import com.miotech.kun.workflow.core.model.worker.WorkerInstance;
import com.miotech.kun.workflow.core.model.worker.WorkerSnapshot;
import com.miotech.kun.workflow.core.publish.EventPublisher;
import com.miotech.kun.workflow.executor.CommonTestBase;
import com.miotech.kun.workflow.executor.WorkerMonitor;
import com.miotech.kun.workflow.testing.event.EventCollector;
import com.miotech.kun.workflow.testing.factory.MockOperatorFactory;
import com.miotech.kun.workflow.testing.factory.MockTaskAttemptFactory;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.PodStatus;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.MixedOperation;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static com.miotech.kun.workflow.executor.kubernetes.KubernetesConstants.KUN_TASK_ATTEMPT_ID;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;

public class PodLifeCycleManagerTest extends CommonTestBase {

    private final Logger logger = LoggerFactory.getLogger(PodLifeCycleManager.class);

    @Inject
    private PodLifeCycleManager podLifeCycleManager;

    @Inject
    private TaskRunDao taskRunDao;

    @Inject
    private TaskDao taskDao;

    @Inject
    private OperatorDao operatorDao;

    @Inject
    private EventBus eventBus;

    private EventCollector eventCollector;

    private FilterWatchListDeletable mockFilter;

    private MixedOperation mockMixedOperation;


    private KubernetesClient client;

    @Override
    protected void configuration() {
        client = mock(KubernetesClient.class);
        Props props = new Props();
        props.put("executor.env", "KUBERNETES");
        props.put("executor.env.version", "1.15");
        props.put("executor.env.logPath", "/server/lib/logs");
        bind(Props.class, props);
        bind(KubernetesClient.class, client);
        bind(WorkerMonitor.class, PodEventMonitor.class);
        bind(EventPublisher.class, new NopEventPublisher());
        bind(EventBus.class, new EventBus());

        super.configuration();
    }

    private void prepareKubernetes() {
        mockMixedOperation = mock(MixedOperation.class);
        doReturn(mockMixedOperation).when(client).pods();
        mockFilter = mock(FilterWatchListDeletable.class);
        doReturn(mockFilter).when(mockMixedOperation).withLabel(any());
        doReturn(mockFilter).when(mockMixedOperation).withLabel(any(), any());
        doReturn(mockFilter).when(mockFilter).withLabel(any());
        doReturn(mockFilter).when(mockFilter).withLabel(any(), any());
    }

    private void mockWatcher(MockPod mockPod) {
        Mockito.doAnswer(invocation -> {
            Watcher<Pod> watcher = invocation.getArgument(0);
            mockPod.addWatcher(watcher);
            return null;
        }).when(mockFilter).watch(any());
    }

    private void mockCreate() {
        Mockito.doAnswer(invocation -> {
            Pod pod = invocation.getArgument(0);
            ObjectMeta objectMeta = pod.getMetadata();
            objectMeta.setName("kubernetes-" + objectMeta.getLabels().get(KUN_TASK_ATTEMPT_ID));
            pod.setMetadata(objectMeta);
            PodStatus podStatus = new PodStatus();
            podStatus.setPhase("Pending");
            pod.setStatus(podStatus);
            return pod;
        }).when(mockMixedOperation).create(any(Pod.class));
    }

    private void mockGet(Pod pod) {
        PodList podList = new PodList();
        List<Pod> items = new ArrayList<>();
        if (pod != null) {
            items.add(pod);
        }
        podList.setItems(items);
        Mockito.doReturn(podList).when(mockFilter).list();
    }

    private void mockStop(MockPod mockPod) {
        Mockito.doAnswer(invocation -> {
            mockPod.cancel();
            return true;
        }).when(mockFilter).delete();
    }

    @Before
    public void init() {
        eventCollector = new EventCollector();
        eventBus.register(eventCollector);
        prepareKubernetes();
        mockCreate();
    }

    @Test
    public void startTaskAttemptShouldSuccess() {
        List<String> podStatusList = Arrays.asList("running", "succeeded");
        TaskAttempt taskAttempt = prepareAttempt();
        mockGet(null);
        MockPod mockPod = new MockPod(podStatusList, taskAttempt.getId());
        mockWatcher(mockPod);
        Thread thread = new Thread(mockPod);
        thread.start();
        podLifeCycleManager.start(taskAttempt);
        awaitUntilAttemptDone(taskAttempt.getId());

        // task_run and task_attempt
        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(taskAttempt.getTaskRun().getId());
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(attemptProps.getStartAt(), is(notNullValue()));
        assertThat(attemptProps.getEndAt(), is(notNullValue()));

        TaskRun taskRun = taskRunDao.fetchLatestTaskRun(taskAttempt.getTaskRun().getTask().getId());
        assertThat(taskRun.getStatus(), is(attemptProps.getStatus()));
        assertThat(taskRun.getStartAt(), is(attemptProps.getStartAt()));
        assertThat(taskRun.getEndAt(), is(attemptProps.getEndAt()));

        assertStatusProgress(taskAttempt.getId(),
                TaskRunStatus.CREATED,
                TaskRunStatus.INITIALIZING,
                TaskRunStatus.RUNNING,
                TaskRunStatus.SUCCESS);
    }

    @Test(expected = IllegalStateException.class)
    public void startTaskAttemptHasRunningShouldThrowException() {
        List<String> podStatusList = Arrays.asList("running", "succeeded");
        TaskAttempt taskAttempt = prepareAttempt();
        MockPod mockPod = new MockPod(podStatusList, taskAttempt.getId());
        mockWatcher(mockPod);
        mockGet(MockPodFactory.create(taskAttempt.getId(), "Running"));
        podLifeCycleManager.start(taskAttempt);

    }


    @Test
    public void stopTaskAttempt() {
        List<String> podStatusList = Arrays.asList("running", "succeeded");
        TaskAttempt taskAttempt = prepareAttempt();
        mockGet(null);
        MockPod mockPod = new MockPod(podStatusList, taskAttempt.getId());
        mockWatcher(mockPod);
        Thread thread = new Thread(mockPod);
        thread.start();
        podLifeCycleManager.start(taskAttempt);
        awaitUntilRunning(taskAttempt.getId());
        Pod runningPod = MockPodFactory.create(taskAttempt.getId(), mockPod.getStatus());
        mockGet(runningPod);
        mockStop(mockPod);
        podLifeCycleManager.stop(taskAttempt);
        awaitUntilAttemptDone(taskAttempt.getId());

        // task_run and task_attempt
        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(taskAttempt.getTaskRun().getId());
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.ABORTED));
        assertThat(attemptProps.getStartAt(), is(notNullValue()));
        assertThat(attemptProps.getEndAt(), is(notNullValue()));

        TaskRun taskRun = taskRunDao.fetchLatestTaskRun(taskAttempt.getTaskRun().getTask().getId());
        assertThat(taskRun.getStatus(), is(attemptProps.getStatus()));
        assertThat(taskRun.getStartAt(), is(attemptProps.getStartAt()));
        assertThat(taskRun.getEndAt(), is(attemptProps.getEndAt()));

        assertStatusProgress(taskAttempt.getId(),
                TaskRunStatus.CREATED,
                TaskRunStatus.INITIALIZING,
                TaskRunStatus.RUNNING,
                TaskRunStatus.ABORTED);

    }

    @Test(expected = IllegalStateException.class)
    public void stopFinishedTaskAttemptShouldThrowException() {
        List<String> podStatusList = Arrays.asList("running", "succeeded");
        TaskAttempt taskAttempt = prepareAttempt();
        mockGet(null);
        MockPod mockPod = new MockPod(podStatusList, taskAttempt.getId());
        mockWatcher(mockPod);
        Thread thread = new Thread(mockPod);
        thread.start();
        podLifeCycleManager.start(taskAttempt);
        awaitUntilAttemptDone(taskAttempt.getId());

        // task_run and task_attempt
        TaskAttemptProps attemptProps = taskRunDao.fetchLatestTaskAttempt(taskAttempt.getTaskRun().getId());
        assertThat(attemptProps.getAttempt(), is(1));
        assertThat(attemptProps.getStatus(), is(TaskRunStatus.SUCCESS));
        assertThat(attemptProps.getStartAt(), is(notNullValue()));
        assertThat(attemptProps.getEndAt(), is(notNullValue()));

        TaskRun taskRun = taskRunDao.fetchLatestTaskRun(taskAttempt.getTaskRun().getTask().getId());
        assertThat(taskRun.getStatus(), is(attemptProps.getStatus()));
        assertThat(taskRun.getStartAt(), is(attemptProps.getStartAt()));
        assertThat(taskRun.getEndAt(), is(attemptProps.getEndAt()));
        assertStatusProgress(taskAttempt.getId(),
                TaskRunStatus.CREATED,
                TaskRunStatus.INITIALIZING,
                TaskRunStatus.RUNNING,
                TaskRunStatus.SUCCESS);

        //stop pod
        Pod finishedPod = MockPodFactory.create(taskAttempt.getId(), mockPod.getStatus());
        mockGet(finishedPod);
        podLifeCycleManager.stop(taskAttempt);


    }


    @Test
    public void testGetSnapshot() {
        List<String> podStatusList = Arrays.asList("running", "succeeded");
        TaskAttempt taskAttempt = prepareAttempt();
        mockGet(null);
        MockPod mockPod = new MockPod(podStatusList, taskAttempt.getId());
        mockWatcher(mockPod);
        Thread thread = new Thread(mockPod);
        thread.start();
        WorkerInstance workerInstance = podLifeCycleManager.start(taskAttempt);
        awaitUntilRunning(taskAttempt.getId());
        Pod runningPod = MockPodFactory.create(taskAttempt.getId(), mockPod.getStatus());
        mockGet(runningPod);
        WorkerSnapshot workerSnapshot = podLifeCycleManager.get(taskAttempt);

        //verify
        assertThat(workerSnapshot.getIns(), is(workerInstance));
        assertThat(workerSnapshot.getStatus(), is(TaskRunStatus.RUNNING));
    }


    private void assertStatusProgress(Long attemptId, TaskRunStatus... asserts) {
        checkArgument(asserts.length > 1);

        List<Event> events = eventCollector.getEvents();

        List<Event> eventsOfAttempt = events.stream()
                .filter(e -> e instanceof TaskAttemptStatusChangeEvent &&
                        ((TaskAttemptStatusChangeEvent) e).getAttemptId() == attemptId)
                .collect(Collectors.toList());

        logger.info(eventsOfAttempt.toString());
        for (int i = 0; i < asserts.length - 1; i++) {
            TaskAttemptStatusChangeEvent event = (TaskAttemptStatusChangeEvent) eventsOfAttempt.get(i);
            assertThat(event.getAttemptId(), is(attemptId));
            assertThat(event.getFromStatus(), is(asserts[i]));
            assertThat(event.getToStatus(), is(asserts[i + 1]));
        }
    }

    private TaskAttempt prepareAttempt() {
        TaskAttempt attempt = MockTaskAttemptFactory.createTaskAttempt();
        long operatorId = attempt.getTaskRun().getTask().getOperatorId();
        Operator op = MockOperatorFactory.createOperator()
                .cloneBuilder()
                .withId(operatorId)
                .withName("Operator_" + operatorId)
                .build();
        operatorDao.createWithId(op, operatorId);
        taskDao.create(attempt.getTaskRun().getTask());
        taskRunDao.createTaskRun(attempt.getTaskRun());
        taskRunDao.createAttempt(attempt);
        return attempt;
    }

    private void awaitUntilAttemptDone(long attemptId) {
        await().atMost(60, TimeUnit.SECONDS).until(() -> {
            Optional<TaskRunStatus> s = taskRunDao.fetchTaskAttemptStatus(attemptId);
            return s.isPresent() && (s.get().isFinished());
        });
    }

    private void awaitUntilRunning(Long attemptId) {
        await().atMost(30, TimeUnit.SECONDS)
                .until(() -> {
                    TaskAttempt attempt = taskRunDao.fetchAttemptById(attemptId).get();
                    return attempt.getStatus().equals(TaskRunStatus.RUNNING);
                });
    }

    class MockPod implements Runnable {
        private Watcher<Pod> watcher;

        private final List<String> podStatusList;

        private final Long taskAttemptId;

        private String status = "Pending";

        private boolean cancel = false;

        public MockPod(List<String> podStatusList, Long taskAttemptId) {
            this.podStatusList = podStatusList;
            this.taskAttemptId = taskAttemptId;
        }

        public void addWatcher(Watcher<Pod> watcher) {
            this.watcher = watcher;
        }

        public String getStatus() {
            return status;
        }

        public void cancel() {
            cancel = true;
        }

        @Override
        public void run() {
            for (int i = 0; i < podStatusList.size(); i++) {
                Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
                if (cancel) {
                    break;
                }
                status = podStatusList.get(0);
                Pod pod = MockPodFactory.create(taskAttemptId, podStatusList.get(i));
                if (watcher != null) {
                    watcher.eventReceived(null, pod);
                }
            }
        }
    }

    private static class NopEventPublisher implements EventPublisher {
        @Override
        public void publish(Event event) {
            // nop
        }
    }
}