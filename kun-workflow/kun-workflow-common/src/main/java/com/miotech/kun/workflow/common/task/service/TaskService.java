package com.miotech.kun.workflow.common.task.service;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.miotech.kun.metadata.core.model.DataStore;
import com.miotech.kun.workflow.common.exception.EntityNotFoundException;
import com.miotech.kun.workflow.common.graph.DirectTaskGraph;
import com.miotech.kun.workflow.common.lineage.node.TaskNode;
import com.miotech.kun.workflow.common.lineage.service.LineageService;
import com.miotech.kun.workflow.common.operator.dao.OperatorDao;
import com.miotech.kun.workflow.common.operator.service.OperatorService;
import com.miotech.kun.workflow.common.task.dao.TaskDao;
import com.miotech.kun.workflow.common.task.dependency.TaskDependencyFunctionProvider;
import com.miotech.kun.workflow.common.task.filter.TaskSearchFilter;
import com.miotech.kun.workflow.common.task.vo.*;
import com.miotech.kun.workflow.core.Scheduler;
import com.miotech.kun.workflow.core.execution.Config;
import com.miotech.kun.workflow.core.execution.ConfigDef;
import com.miotech.kun.workflow.core.execution.KunOperator;
import com.miotech.kun.workflow.core.execution.Resolver;
import com.miotech.kun.workflow.core.model.operator.Operator;
import com.miotech.kun.workflow.core.model.task.*;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import com.miotech.kun.workflow.utils.JSONUtils;
import com.miotech.kun.workflow.utils.WorkflowIdGenerator;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.lang.String.format;

@Singleton
public class TaskService {
    private static final Logger logger = LoggerFactory.getLogger(TaskService.class);

    private static final String TASK_ID_SHOULD_NOT_BE_NULL = "Invalid argument `taskId`: null";

    private static final String TASK_SHOULD_NOT_BE_NULL = "Invalid argument `task`: null";

    private final TaskDao taskDao;

    private final OperatorDao operatorDao;

    private final OperatorService operatorService;

    private final TaskDependencyFunctionProvider dependencyFunctionProvider;

    private final Scheduler scheduler;

    private final LineageService lineageService;

    private final String DEFAULT_QUEUE = "default";


    @Inject
    public TaskService(
            TaskDao taskDao,
            OperatorDao operatorDao,
            OperatorService operatorService,
            TaskDependencyFunctionProvider dependencyFunctionProvider,
            Scheduler scheduler,
            LineageService lineageService
    ) {
        this.taskDao = taskDao;
        this.operatorDao = operatorDao;
        this.operatorService = operatorService;
        this.dependencyFunctionProvider = dependencyFunctionProvider;
        this.scheduler = scheduler;
        this.lineageService = lineageService;
    }

    /* ---------------------------------------- */
    /* ----------- public methods ------------ */
    /* ---------------------------------------- */

    /**
     * Create a task by given task properties value object, will throw exception if required properties or binding operator not found
     *
     * @param vo a task properties value object
     * @return created task
     */
    public Task createTask(TaskPropsVO vo) {
        // 1. Validate integrity of TaskPropsVO
        validateTaskPropsVOIntegrity(vo);

        // 2. Binding operator should exists. If not, throw exception.
        if (!operatorDao.fetchById(vo.getOperatorId()).isPresent()) {
            throw new EntityNotFoundException(String.format("Cannot create task with operator id: %d, target operator not found.", vo.getOperatorId()));
        }

        // 3. Task name should not conflict
        if (taskDao.fetchByName(vo.getName()).isPresent()) {
            throw new IllegalArgumentException(String.format("Cannot create task with duplicated name: \"%s\"", vo.getName()));
        }

        // 4. convert value object to task object and assign a new task id
        Task taskWithProps = convertTaskPropsVoToTask(vo);
        Task task = taskWithProps.cloneBuilder()
                .withId(WorkflowIdGenerator.nextTaskId())
                .build();
        // 5. persist with DAO
        taskDao.create(task);

        // 6. update lineage
        updateLineageGraphOnTaskCreate(task);
        return task;
    }

    /**
     * @param task
     */
    private void checkCycleDependencies(Task task) {
        List<Long> cycleDependencies = taskDao.getCycleDependencies(task.getId(), task.getDependencies().
                stream().map(TaskDependency::getDownstreamTaskId).collect(Collectors.toList()));
        if (cycleDependencies.size() != 0) {
            throw new IllegalArgumentException("create task:" + task.getId() + ",has cycle dependencies:" + cycleDependencies);
        }
    }


    private Config parseConfig(TaskPropsVO vo) {
        ConfigDef configDef = operatorService.getOperatorConfigDef(vo.getOperatorId());
        // populate default values
        Config config = new Config(configDef, vo.getConfig().getValues());
        if (vo.getScheduleConf().getType().equals(ScheduleType.NONE)) {
            return config;
        }
        checkConfig(configDef, config);
        return config;
    }

    private void checkConfig(ConfigDef configDef, Config config) {
        for (ConfigDef.ConfigKey configKey : configDef.configKeys()) {
            String name = configKey.getName();
            if (configKey.isRequired() && !config.contains(name)) {
                throw new IllegalArgumentException(format("Configuration %s is required but not specified", name));
            }
        }
    }

    private void checkRuntimeConfig(Map<String, Object> runtimeConfig, Task task) {
        ConfigDef configDef = operatorService.getOperatorConfigDef(task.getOperatorId());
        Config config = new Config(runtimeConfig);
        config.validateBy(configDef);
        for (ConfigDef.ConfigKey configKey : configDef.configKeys()) {
            String name = configKey.getName();
            if (!configKey.isReconfigurable() && config.contains(name)) {
                throw new IllegalArgumentException(format("Configuration %s should not be reconfigured.", name));
            }
        }
    }

    public Task partialUpdateTask(Long taskId, TaskPropsVO vo) {
        // 1. Validate arguments
        Preconditions.checkNotNull(taskId, TASK_ID_SHOULD_NOT_BE_NULL);
        Preconditions.checkNotNull(vo, "Cannot perform update with vo: null");

        Task task = fetchById(taskId);
        Task taskToUpdate = task.cloneBuilder()
                .withId(taskId)
                .withName(StringUtils.isEmpty(vo.getName()) ? task.getName() : vo.getName())
                .withDescription(StringUtils.isEmpty(vo.getDescription()) ? task.getDescription() : vo.getDescription())
                .withOperatorId(Objects.isNull(vo.getOperatorId()) ? task.getOperatorId() : vo.getOperatorId())
                .withConfig(Objects.isNull(vo.getConfig()) ? task.getConfig() : vo.getConfig())
                .withScheduleConf(Objects.isNull(vo.getScheduleConf()) ? task.getScheduleConf() : vo.getScheduleConf())
                .withDependencies(vo.getDependencies() == null ? task.getDependencies() : parseDependencyVO(vo.getDependencies()))
                .withTags(vo.getTags() == null ? task.getTags() : vo.getTags())
                .withQueueName(vo.getQueueName() == null ? task.getQueueName() : vo.getQueueName())
                .withPriority(vo.getPriority() == null ? TaskPriority.MEDIUM.getPriority() : TaskPriority.valueOf(vo.getPriority()).getPriority())
                .build();

        // 4. perform update
        return fullUpdateTask(taskToUpdate);
    }

    public Task fullUpdateTaskById(Long taskId, TaskPropsVO vo) {
        Preconditions.checkNotNull(taskId, TASK_ID_SHOULD_NOT_BE_NULL);
        Preconditions.checkNotNull(vo, "Cannot perform update with vo: null");

        Config config = parseConfig(vo);

        Task task = fetchById(taskId).cloneBuilder()
                .withId(taskId)
                .withName(vo.getName())
                .withScheduleConf(vo.getScheduleConf())
                .withDependencies(parseDependencyVO(vo.getDependencies()))
                .withConfig(config)
                .withDescription(vo.getDescription())
                .withOperatorId(vo.getOperatorId())
                .withQueueName(vo.getQueueName() == null ? DEFAULT_QUEUE : vo.getQueueName())
                .withTags(vo.getTags())
                .withPriority(vo.getPriority() == null ? TaskPriority.MEDIUM.getPriority() : TaskPriority.valueOf(vo.getPriority()).getPriority())
                .build();
        // check dependency
        checkCycleDependencies(task);

        return fullUpdateTask(task);
    }

    public Task fullUpdateTask(Task task) {
        // 1. Validate task object integrity
        validateTaskIntegrity(task);

        // 2. Binding operator should exists. If not, throw exception.
        if (!operatorDao.fetchById(task.getOperatorId()).isPresent()) {
            throw new EntityNotFoundException(String.format("Cannot create task with operator id: %d, target operator not found.", task.getOperatorId()));
        }

        // 3. check dependency
        checkCycleDependencies(task);

        // 4. Update target task. If there is no task affected (task not exists), throw exception
        boolean taskUpdated = taskDao.update(task);
        if (!taskUpdated) {
            throw new EntityNotFoundException(String.format("Cannot perform update on non-exist task with id: %d", task.getId()));
        }

        // 5. Fetch updated task
        Task updatedTask = taskDao.fetchById(task.getId()).orElseThrow(IllegalStateException::new);

        // 6. Update lineage graph
        updateLineageGraphOnTaskUpdate(updatedTask);

        return updatedTask;
    }

    public TaskDAGVO getNeighbors(Long taskId, int upstreamLevel, int downstreamLevel) {
        Preconditions.checkArgument(0 <= upstreamLevel && upstreamLevel <= 5, "upstreamLevel should be non negative and no greater than 5");
        Preconditions.checkArgument(0 <= downstreamLevel && downstreamLevel <= 5, "downstreamLevel should be non negative and no greater than 5");

        Task task = fetchById(taskId);
        List<Task> result = new ArrayList<>();
        result.add(task);
        if (upstreamLevel > 0) {
            result.addAll(taskDao.fetchUpstreamTasks(task, upstreamLevel));
        }
        if (downstreamLevel > 0) {
            result.addAll(taskDao.fetchDownstreamTasks(task, downstreamLevel));
        }

        List<TaskDependency> edges = result.stream()
                .flatMap(x -> x.getDependencies().stream())
                .collect(Collectors.toList());
        return new TaskDAGVO(result, edges);
    }

    /**
     * Fetch task page with filters
     *
     * @param filters
     * @return
     */
    public PaginationVO<Task> fetchTasksByFilters(TaskSearchFilter filters) {
        Preconditions.checkNotNull(filters, "Invalid argument `filters`: null");
        return PaginationVO.<Task>newBuilder()
                .withPageNumber(filters.getPageNum())
                .withPageSize(filters.getPageSize())
                .withRecords(taskDao.fetchWithFilters(filters))
                .withTotalCount(taskDao.fetchTotalCountWithFilters(filters))
                .build();
    }

    /**
     * Fetch tasks of tasks that matches filter constraints
     *
     * @param filters filter instance
     * @return total number of records that matches filter constraints
     */
    public Integer fetchTaskTotalCount(TaskSearchFilter filters) {
        Preconditions.checkNotNull(filters, "Invalid argument `filters`: null");
        return taskDao.fetchTotalCountWithFilters(filters);
    }

    /**
     * Fetch single task by id
     *
     * @param taskId Task id
     * @return task
     */
    public Task fetchById(Long taskId) {
        Preconditions.checkNotNull(taskId, TASK_ID_SHOULD_NOT_BE_NULL);
        return taskDao.fetchById(taskId)
                .orElseThrow(() ->
                        new EntityNotFoundException(String.format("Cannot find task with id: %s", taskId))
                );
    }

    /**
     * Fetch tasks by given ids
     *
     * @param taskIds ids of tasks
     * @returna a map from id to optional task object
     */
    public Map<Long, Optional<Task>> fetchByIds(Set<Long> taskIds) {
        Preconditions.checkNotNull(taskIds, "Invalid argument `taskIds`: null");
        return taskDao.fetchByIds(taskIds);
    }

    public void deleteTask(Task task) {
        // TODO: recheck this
        validateTaskIntegrity(task);
        this.deleteTaskById(task.getId());
        updateLineageGraphOnTaskDelete(task);
    }

    public void deleteTaskById(Long taskId) {
        boolean entityDeleted = taskDao.deleteById(taskId);
        if (!entityDeleted) {
            throw new EntityNotFoundException(String.format("Cannot delete non-exist task with id: %d", taskId));
        }
    }

    public List<Long> runTasks(List<RunTaskVO> runTaskVOs) {
        Map<Long, RunTaskVO> rtvMap = runTaskVOs.stream()
                .collect(Collectors.toMap(RunTaskVO::getTaskId, Function.identity()));

        // fetch tasks
        Map<Long, Optional<Task>> fetched = taskDao.fetchByIds(rtvMap.keySet());
        List<Task> tasks = fetched.entrySet().stream()
                .map(e -> e.getValue()
                        .orElseThrow(() -> new IllegalArgumentException("Task does not exist for id: " + e.getKey()))
                )
                .collect(Collectors.toList());

        // create graph
        DirectTaskGraph graph = new DirectTaskGraph(tasks);

        // build taskRunEnv
        TaskRunEnv.Builder envBuilder = TaskRunEnv.newBuilder();
        for (Task t : tasks) {
            Long taskId = t.getId();
            Optional<Operator> operatorOptional = operatorDao.fetchById(t.getOperatorId());
            if (!operatorOptional.isPresent()) {
                throw new EntityNotFoundException(String.format("Cannot create task with operator id: %d, target operator not found.", t.getOperatorId()));
            }
            Map<String, Object> config = rtvMap.get(taskId).getConfig();
            if (config == null) {
                config = Collections.emptyMap();
            }
            checkRuntimeConfig(config, t);
            envBuilder.addConfig(taskId, config);
        }

        // run graph
        List<TaskRun> taskRuns = scheduler.run(graph, envBuilder.build());

        return taskRuns.stream().map(TaskRun::getId).collect(Collectors.toList());
    }

    /* ---------------------------------------- */
    /* ----------- private methods ------------ */
    /* ---------------------------------------- */

    private void validateTaskPropsVONotNull(TaskPropsVO vo) {
        Preconditions.checkNotNull(vo, "Invalid TaskPropsVO argument: null");
    }

    private List<TaskDependency> parseDependencyVO(List<TaskDependencyVO> vo) {
        return vo.stream().map(x -> new TaskDependency(x.getUpstreamTaskId(),
                x.getDownstreamTaskId(), dependencyFunctionProvider.from(
                x.getDependencyFunction()),
                x.getDependencyLevel() == null ? DependencyLevel.STRONG : DependencyLevel.resolve(x.getDependencyLevel())))
                .collect(Collectors.toList());
    }

    private Task convertTaskPropsVoToTask(TaskPropsVO vo) {
        Config config = parseConfig(vo);
        return Task.newBuilder()
                .withName(vo.getName())
                .withDescription(vo.getDescription())
                .withOperatorId(vo.getOperatorId())
                .withScheduleConf(vo.getScheduleConf())
                .withConfig(config)
                .withDependencies(parseDependencyVO(vo.getDependencies()))
                .withTags(vo.getTags())
                .withQueueName(vo.getQueueName() == null ? DEFAULT_QUEUE : vo.getQueueName())
                .withPriority(vo.getPriority() == null ? TaskPriority.MEDIUM.getPriority() : TaskPriority.valueOf(vo.getPriority()).getPriority())
                .build();
    }

    private void validateTaskIntegrity(Task task) {
        Preconditions.checkNotNull(task.getId(), "Invalid task with property `id`: null");
        TaskPropsVO vo = TaskPropsVO.from(task);
        validateTaskPropsVOIntegrity(vo);
    }

    private void validateTaskPropsVOIntegrity(TaskPropsVO vo) {
        validateTaskPropsVONotNull(vo);
        Preconditions.checkArgument(Objects.nonNull(vo.getName()), "Invalid task property object with property `name`: null");
        Preconditions.checkArgument(Objects.nonNull(vo.getDescription()), "Invalid task property object with property `description`: null");
        Preconditions.checkArgument(Objects.nonNull(vo.getOperatorId()), "Invalid task property object with property `operatorId`: null");
        Preconditions.checkArgument(Objects.nonNull(vo.getScheduleConf()), "Invalid task property object with property `scheduleConf`: null");
        Preconditions.checkArgument(Objects.nonNull(vo.getConfig()), "Invalid task property object with property `config`: null");
        Preconditions.checkArgument(Objects.nonNull(vo.getDependencies()), "Invalid task property object with property `dependencies`: null");
        Preconditions.checkArgument(Objects.nonNull(vo.getTags()), "Invalid task property object with property `tags`: null");
        // Validate tags property of task VO
        Set<String> tagKeys = new HashSet<>();
        vo.getTags().forEach(tag -> {
            if (tagKeys.contains(tag.getKey())) {
                throw new IllegalArgumentException(String.format("Found key conflict: \"%s\"", tag.getKey()));
            }
            // else
            tagKeys.add(tag.getKey());
        });
    }

    /**
     * Perform update on lineage graph during update of a task model
     *
     * @param task {@link Task} model instance
     * @throws EntityNotFoundException if operator not found
     * @throws NullPointerException    if argument task is null, or id of task is null, or resolver of operator is not defined
     */
    private void updateLineageGraphOnTaskUpdate(Task task) {
        Preconditions.checkNotNull(task, TASK_SHOULD_NOT_BE_NULL);
        // Simply remove the original task nodes then re-create one
        logger.debug("Performing update on lineage graph during update of task: id = {}, name = {}, config = {}",
                task.getId(), task.getName(), JSONUtils.toJsonString(task.getConfig()));
        updateLineageGraphOnTaskDelete(task);
        updateLineageGraphOnTaskCreate(task);
    }

    /**
     * Perform update on lineage graph during creation of a task model
     *
     * @param task {@link Task} model instance
     * @throws EntityNotFoundException if operator not found
     * @throws NullPointerException    if argument task is null or id of task is null
     */
    private void updateLineageGraphOnTaskCreate(Task task) {
        Preconditions.checkNotNull(task, TASK_SHOULD_NOT_BE_NULL);
        // load operator resolver
        Resolver resolver = getResolverByOperatorId(task.getOperatorId());

        // Load upstream & downstream data stores
        List<DataStore> upstreamDatastore = resolver.resolveUpstreamDataStore(task.getConfig());
        List<DataStore> downstreamDataStore = resolver.resolveDownstreamDataStore(task.getConfig());
        logger.debug("For task id = {}, resolved {} upstream datastores and {} downstream datastores.",
                task.getId(), upstreamDatastore.size(), downstreamDataStore.size());
        lineageService.updateTaskLineage(task, upstreamDatastore, downstreamDataStore);
    }

    /**
     * Perform update on lineage graph during deletion of a task model
     *
     * @param task {@link Task} model instance
     * @throws NullPointerException if argument task is null or id of task is null
     */
    private void updateLineageGraphOnTaskDelete(Task task) {
        Preconditions.checkNotNull(task, TASK_SHOULD_NOT_BE_NULL);
        // Is this task node already exists in graph?
        logger.debug("Clearing related lineage graph info for task with id = {}", task.getId());
        Optional<TaskNode> taskNodeOptional = lineageService.fetchTaskNodeById(task.getId());
        if (!taskNodeOptional.isPresent()) {
            return;
        }
        // else
        TaskNode taskNode = taskNodeOptional.get();
        lineageService.deleteTaskNode(taskNode.getTaskId());
    }

    /**
     * Get the {@link Resolver} of operator by given id of target operator
     *
     * @param operatorId id of target operator
     * @return the resolver of operator
     * @throws EntityNotFoundException if operator not found
     * @throws NullPointerException    if argument operatorId is null, or resolver of operator is not defined
     */
    private Resolver getResolverByOperatorId(Long operatorId) {
        Preconditions.checkNotNull(operatorId, "Invalid argument `operatorId`: null");
        KunOperator operator = operatorService.loadOperator(operatorId);
        // Does this operator exist?
        if (Objects.isNull(operator)) {
            throw new EntityNotFoundException(String.format("Cannot find operator with id: %s", operatorId));
        }
        // Does the resolver exist?
        if (Objects.isNull(operator.getResolver())) {
            throw new NullPointerException(String.format("Cannot get resolver from Kun operator with id: %s", operatorId));
        }
        return operator.getResolver();
    }
}
