package com.miotech.kun.workflow.scheduler;

import com.google.common.collect.Lists;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;
import com.miotech.kun.commons.utils.EventConsumer;
import com.miotech.kun.commons.utils.EventLoop;
import com.miotech.kun.commons.utils.InitializingBean;
import com.miotech.kun.workflow.common.graph.DirectTaskGraph;
import com.miotech.kun.workflow.common.operator.service.OperatorService;
import com.miotech.kun.workflow.common.taskrun.dao.TaskRunDao;
import com.miotech.kun.workflow.common.tick.TickDao;
import com.miotech.kun.workflow.common.variable.service.VariableService;
import com.miotech.kun.workflow.core.event.Event;
import com.miotech.kun.workflow.core.event.TickEvent;
import com.miotech.kun.workflow.core.execution.Config;
import com.miotech.kun.workflow.core.execution.ConfigDef;
import com.miotech.kun.workflow.core.model.common.SpecialTick;
import com.miotech.kun.workflow.core.model.common.Tick;
import com.miotech.kun.workflow.core.model.task.*;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import com.miotech.kun.workflow.core.model.taskrun.TaskRunStatus;
import com.miotech.kun.workflow.utils.DateTimeUtils;
import com.miotech.kun.workflow.utils.WorkflowIdGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;

@Singleton
public class TaskSpawner implements InitializingBean {
    private static final Logger logger = LoggerFactory.getLogger(TaskSpawner.class);

    private final TaskManager taskManager;

    private final TaskRunDao taskRunDao;

    private final OperatorService operatorService;

    private final VariableService variableService;

    private final EventBus eventBus;

    private final TickDao tickDao;

    private final Deque<TaskGraph> graphs;
    private final InnerEventLoop eventLoop;

    @Inject
    public TaskSpawner(TaskManager taskManager,
                       TaskRunDao taskRunDao,
                       OperatorService operatorService,
                       VariableService variableService,
                       EventBus eventBus,
                       TickDao tickDao) {
        this.taskManager = taskManager;
        this.taskRunDao = taskRunDao;
        this.operatorService = operatorService;
        this.variableService = variableService;
        this.eventBus = eventBus;

        this.graphs = new ConcurrentLinkedDeque<>();
        this.eventLoop = new InnerEventLoop();
        this.eventBus.register(this.eventLoop);
        this.tickDao = tickDao;
        this.eventLoop.start();
    }

    /* ----------- public methods ------------ */

    public void schedule(TaskGraph graph) {
        checkNotNull(graph, "graph should not be null.");
        graphs.add(graph);
    }

    public void init() {
        List<TaskRun> recoverTaskRunList = taskRunDao.fetchTaskRunListWithoutAttempt();
        logger.info("submit unStartedTaskRun = {}", recoverTaskRunList);
        if (recoverTaskRunList.size() > 0) {
            submit(recoverTaskRunList);
        }
    }

    public List<TaskRun> run(TaskGraph graph) {
        return run(graph, TaskRunEnv.EMPTY);
    }

    public List<TaskRun> run(TaskGraph graph, Tick tick) {
        checkNotNull(graph, "graph should not be null.");
        return spawn(Lists.newArrayList(graph), tick, TaskRunEnv.EMPTY);
    }

    public List<TaskRun> run(TaskGraph graph, TaskRunEnv env) {
        checkNotNull(graph, "graph should not be null.");
        checkNotNull(env, "env should not be null.");
        checkState(graph instanceof DirectTaskGraph, "Only DirectTaskGraph is accepted.");
        Tick current = SpecialTick.NULL;
        logger.info("run task directly size = {}", ((DirectTaskGraph) graph).getTasks().size());
        return spawn(Lists.newArrayList(graph), current, env);
    }

    public boolean rerun(TaskRun taskRun) {
        return taskManager.retry(taskRun);
    }

    /* ----------- private methods ------------ */

    private void handleTickEvent(TickEvent tickEvent) {
        logger.debug("handle TickEvent tickEvent={}", tickEvent);
        spawn(graphs, tickEvent.getTick(), TaskRunEnv.EMPTY);
    }

    private List<TaskRun> spawn(Collection<TaskGraph> graphs, Tick tick, TaskRunEnv env) {
        List<TaskRun> taskRuns = new ArrayList<>();
        OffsetDateTime currentTickTime = DateTimeUtils.now();
        Map<TaskGraph, List<TaskRun>> graphTaskRuns = new HashMap<>();
        for (TaskGraph graph : graphs) {
            List<Task> tasksToRun = graph.tasksScheduledAt(tick).stream().
                    filter(task -> task.shouldSchedule(tick, currentTickTime)).collect(Collectors.toList());
            List<TaskRun> taskRunList = createTaskRuns(tasksToRun, tick, env);
            taskRuns.addAll(taskRunList);
            List<Task> taskList = taskRunList.stream().map(TaskRun::getTask).collect(Collectors.toList());
            logger.debug("tasks to run: {}, at tick {}", taskList, tick);
            graphTaskRuns.put(graph, taskRunList);

        }
        logger.debug("to save created TaskRuns. TaskRun={}", taskRuns);
        taskRunDao.createTaskRuns(graphTaskRuns);
        if (tick != SpecialTick.NULL) {
            logger.debug("to save checkpoint. checkpoint = {}", tick);
            tickDao.saveCheckPoint(tick);
        }
        logger.debug("to submit created TaskRuns. TaskRuns={}", taskRuns);
        if (taskRuns.size() > 0) {
            submit(taskRuns);
        }
        return taskRuns;
    }


    //幂等，重放tick不会创建新的taskRun
    private List<TaskRun> createTaskRuns(List<Task> tasks, Tick tick, TaskRunEnv env) {
        List<TaskRun> results = new ArrayList<>(tasks.size());
        for (Task task : tasks) {
            List<TaskRun> upstreamTaskRun = resolveDependencies(task, tick, results);
            try {
                if (tick == SpecialTick.NULL) {
                    results.add(createTaskRun(task, tick, env.getConfig(task.getId()), upstreamTaskRun));
                } else {
                    TaskRun taskRun = taskRunDao.fetchTaskRunByTaskAndTick(task.getId(), tick);
                    if (taskRun == null) {
                        results.add(createTaskRun(task, tick, env.getConfig(task.getId()), upstreamTaskRun));
                    } else {
                        results.add(taskRun);
                    }
                }
            } catch (Exception e) {
                logger.error("create taskRun failed , taskId = {}", task.getId(), e);
            }

        }
        return results;
    }


    private TaskRun createTaskRun(Task task, Tick tick, Map<String, Object> runtimeConfig, List<TaskRun> upstreamTaskRuns) {
        Long taskRunId = WorkflowIdGenerator.nextTaskRunId();
        Config config = prepareConfig(task, task.getConfig(), runtimeConfig);
        ScheduleType scheduleType = task.getScheduleConf().getType();
        if (tick == SpecialTick.NULL) {
            tick = SpecialTick.NULL.toTick();
            scheduleType = ScheduleType.NONE;
        }

        List<Long> upstreamTaskRunIds = upstreamTaskRuns.stream().map(TaskRun::getId).collect(Collectors.toList());
        TaskRun taskRun = TaskRun.newBuilder()
                .withId(taskRunId)
                .withTask(task)
                .withConfig(config)
                .withScheduledTick(tick)
                .withScheduleType(scheduleType)
                .withQueueName(task.getQueueName())
                .withPriority(task.getPriority())
                .withDependentTaskRunIds(upstreamTaskRunIds)
                .withStatus(resolveTaskRunUpstreamStatus(task,upstreamTaskRuns))
                .build();
        logger.debug("TaskRun is created successfully TaskRun={}, Task={}, Tick={}.", taskRun, task, tick);
        return taskRun;
    }

    private TaskRunStatus resolveTaskRunUpstreamStatus(Task task,List<TaskRun> upstreamTaskRuns) {
        if (task.getDependencies().size() > upstreamTaskRuns.size()) {
            logger.error("dependency not satisfy, taskId = {}", task.getId());
            return TaskRunStatus.UPSTREAM_NOT_FOUND;
        }
        for (TaskRun upstreamTaskRun : upstreamTaskRuns) {
            if (upstreamTaskRun.getStatus().isFailure()
                    || upstreamTaskRun.getStatus().equals(TaskRunStatus.UPSTREAM_FAILED)) {
                return TaskRunStatus.UPSTREAM_FAILED;
            }
        }
        return TaskRunStatus.CREATED;
    }

    private List<TaskRun> resolveDependencies(Task task, Tick tick, List<TaskRun> others) {
        return task.getDependencies().stream()
                .flatMap(dependency -> {
                    DependencyFunction depFunc = dependency.getDependencyFunction();
                    return depFunc.resolveDependency(task, dependency.getUpstreamTaskId(), tick, others)
                            .stream();
                }).collect(Collectors.toList());
    }

    private Config prepareConfig(Task task, Config config, Map<String, Object> runtimeConfig) {
        ConfigDef configDef = operatorService.getOperatorConfigDef(task.getOperatorId());

        // populate default values
        config = new Config(configDef, config.getValues());

        // override by runtime config
        Config rtConfig = new Config(runtimeConfig);
        rtConfig.validateBy(configDef);
        Config mergedConfig = config.overrideBy(rtConfig);

        // render variables
        Config finalConfig = variableService.renderConfig(mergedConfig);

        // validate final config
        validateConfig(configDef, finalConfig, rtConfig);

        return finalConfig;
    }

    private void validateConfig(ConfigDef configDef, Config finalConfig, Config runtimeConfig) {
        for (ConfigDef.ConfigKey configKey : configDef.configKeys()) {
            String name = configKey.getName();
            if (configKey.isRequired() && !finalConfig.contains(name)) {
                throw new IllegalArgumentException(format("Configuration %s is required but not specified", name));
            }

            if (!configKey.isReconfigurable() && runtimeConfig.contains(name)) {
                throw new IllegalArgumentException(format("Configuration %s should not be reconfigured.", name));
            }
        }
    }

    private void save(List<TaskRun> taskRuns) {
        taskRunDao.createTaskRuns(taskRuns);
    }

    private void submit(List<TaskRun> taskRuns) {
        taskManager.submit(taskRuns);
    }

    private class InnerEventLoop extends EventLoop<Long, Event> {
        public InnerEventLoop() {
            super("task-spawner");
            addConsumers(Lists.newArrayList(
                    new EventConsumer<Long, Event>() {
                        @Override
                        public void onReceive(Event event) {
                            handleTickEvent((TickEvent) event);
                        }
                    }
            ));
        }

        @Subscribe
        public void onReceive(TickEvent event) {
            post(1L, event);
        }
    }

    @Override
    public void afterPropertiesSet() {
        init();
    }
}
