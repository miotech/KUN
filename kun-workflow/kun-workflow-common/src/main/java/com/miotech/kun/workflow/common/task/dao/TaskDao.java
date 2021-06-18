package com.miotech.kun.workflow.common.task.dao;

import com.cronutils.model.Cron;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.miotech.kun.commons.db.DatabaseOperator;
import com.miotech.kun.commons.db.ResultSetMapper;
import com.miotech.kun.commons.db.sql.DefaultSQLBuilder;
import com.miotech.kun.commons.db.sql.SQLBuilder;
import com.miotech.kun.commons.utils.ExceptionUtils;
import com.miotech.kun.workflow.common.task.dependency.TaskDependencyFunctionProvider;
import com.miotech.kun.workflow.common.task.filter.TaskSearchFilter;
import com.miotech.kun.workflow.common.task.vo.TagVO;
import com.miotech.kun.workflow.core.execution.Config;
import com.miotech.kun.workflow.core.model.common.Tag;
import com.miotech.kun.workflow.core.model.common.Tick;
import com.miotech.kun.workflow.core.model.task.DependencyLevel;
import com.miotech.kun.workflow.core.model.task.ScheduleConf;
import com.miotech.kun.workflow.core.model.task.Task;
import com.miotech.kun.workflow.core.model.task.TaskDependency;
import com.miotech.kun.workflow.utils.CronUtils;
import com.miotech.kun.workflow.utils.JSONUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class TaskDao {

    private final Logger logger = LoggerFactory.getLogger(TaskDao.class);

    public static final String TASK_TABLE_NAME = "kun_wf_task";

    public static final String TICK_TASK_MAPPING_TABLE_NAME = "kun_wf_tick_task_mapping";

    public static final String TASK_MODEL_NAME = "tasks";

    public static final String TICK_TASK_MAPPING_TABLE_ALIAS = "tick_task_mapping";

    public static final String TASK_RELATION_TABLE_NAME = "kun_wf_task_relations";

    public static final String TASK_RELATION_MODEL_NAME = "task_relation";

    public static final String TASK_TAGS_TABLE_NAME = "kun_wf_task_tags";

    public static final String TASK_TAGS_MODEL_NAME = "task_tags";

    private static final List<String> taskCols = ImmutableList.of("id", "name", "description", "operator_id", "config", "schedule", "queue_name", "priority");

    private static final List<String> taskTagCols = ImmutableList.of("task_id", "tag_key", "tag_value");

    private static final List<String> tickTaskCols = ImmutableList.of("task_id", "scheduled_tick");

    private static final List<String> taskRelationCols = ImmutableList.of("upstream_task_id", "downstream_task_id", "dependency_function", "dependency_level");

    private static final String TASK_ID_QUERY = " task_id = ? ";
    private final DatabaseOperator dbOperator;

    private final TaskDependencyFunctionProvider functionProvider;

    @Inject
    public TaskDao(DatabaseOperator dbOperator, TaskDependencyFunctionProvider functionProvider) {
        this.dbOperator = dbOperator;
        this.functionProvider = functionProvider;
    }

    public static List<String> getTaskCols() {
        return taskCols;
    }

    private void insertTickTaskRecordByScheduleConf(Long taskId, ScheduleConf scheduleConf) {
        boolean shouldInsertTickTask;

        switch (scheduleConf.getType()) {
            case ONESHOT:
            case SCHEDULED:
                shouldInsertTickTask = true;
                break;
            default:
                shouldInsertTickTask = false;
        }

        if (shouldInsertTickTask) {
            String cronExpression = scheduleConf.getCronExpr();
            Cron cron = CronUtils.convertStringToCron(cronExpression);

            List<String> tickTaskColumns = new ImmutableList.Builder<String>()
                    .addAll(tickTaskCols)
                    .build();

            String tickTaskInsertionSql = DefaultSQLBuilder.newBuilder()
                    .insert(tickTaskColumns.toArray(new String[0]))
                    .into(TICK_TASK_MAPPING_TABLE_NAME)
                    .asPrepared()
                    .getSQL();

            Optional<OffsetDateTime> nextExecutionTimeOptional = CronUtils.getNextExecutionTimeFromNow(cron);
            String formattedScheduleTick;
            if (nextExecutionTimeOptional.isPresent()) {
                OffsetDateTime nextExecutionTime = nextExecutionTimeOptional.get();
                formattedScheduleTick = (new Tick(nextExecutionTime)).toString();
            } else {
                throw new RuntimeException(
                        String.format("Cannot compute next execution time for cron expression: \"%s\" (Task ID: %s)", cronExpression, taskId)
                );
            }

            dbOperator.update(
                    tickTaskInsertionSql,
                    taskId,
                    formattedScheduleTick
            );
        }
    }

    private void updateTaskUpstreamDependencies(Task task, List<TaskDependency> taskDependencies) {
        Preconditions.checkNotNull(task, "Invalid argument `task`: null");
        Preconditions.checkNotNull(taskDependencies, "Invalid argument `taskDependencies`: null");
        // 1. Clear all previous task upstream relations, if any
        String sqlRemove = DefaultSQLBuilder.newBuilder()
                .delete()
                .from(TASK_RELATION_TABLE_NAME)
                .where("downstream_task_id = ?")
                .getSQL();

        // 2. then insert updated task upstream relations
        String sqlInsert = DefaultSQLBuilder.newBuilder()
                .insert(taskRelationCols.toArray(new String[0]))
                .into(TASK_RELATION_TABLE_NAME)
                .valueSize(taskRelationCols.size())
                .asPrepared()
                .getSQL();

        // 3. execute with database operator
        dbOperator.update(sqlRemove, task.getId());
        if (!taskDependencies.isEmpty()) {
            Object[][] params = taskDependencies
                    .stream()
                    .map(taskDependency -> new Object[]{
                            // upstream_task_id
                            taskDependency.getUpstreamTaskId(),
                            // downstream_task_id
                            task.getId(),
                            // dependency_function
                            taskDependency.getDependencyFunction().toFunctionType(),
                            taskDependency.getDependencyLevel().name()
                    })
                    .collect(Collectors.toList())
                    .toArray(new Object[0][0]);
            dbOperator.batch(sqlInsert, params);
        }
    }

    private enum DependencyDirection {
        UPSTREAM,
        DOWNSTREAM
    }

    private void updateTaskTags(Task task, List<Tag> tags) {
        Preconditions.checkNotNull(task, "Invalid argument `task`: null");
        Preconditions.checkNotNull(tags, "Invalid argument `tags`: null");
        // 1. Clear all previous task tags, if any
        String sqlRemove = DefaultSQLBuilder.newBuilder()
                .delete()
                .from(TASK_TAGS_TABLE_NAME)
                .where("task_id = ?")
                .getSQL();

        // 2. then insert updated task tags
        String sqlInsert = DefaultSQLBuilder.newBuilder()
                .insert(taskTagCols.toArray(new String[0]))
                .into(TASK_TAGS_TABLE_NAME)
                .valueSize(taskTagCols.size())
                .asPrepared()
                .getSQL();

        // 3. execute with database operator
        dbOperator.update(sqlRemove, task.getId());
        if (!tags.isEmpty()) {
            Object[][] params = tags
                    .stream()
                    .map(tag -> new Object[]{
                            // task_id
                            task.getId(),
                            // tag_key
                            tag.getKey(),
                            // tag_value
                            tag.getValue()
                    })
                    .collect(Collectors.toList())
                    .toArray(new Object[0][0]);
            dbOperator.batch(sqlInsert, params);
        }
    }

    /**
     * Internal use only. Retrieve task ids within the maximum distance from the given source task.
     *
     * @param srcTask        source task
     * @param distance       maximum distance
     * @param queryDirection upstream or downstream
     * @param includeSelf    include source task or not
     * @return Set of task ids in range
     */
    private Set<Long> retrieveTaskIdsWithinDependencyDistance(Task srcTask, int distance, DependencyDirection queryDirection, boolean includeSelf) {
        Preconditions.checkNotNull(srcTask, "Invalid argument `srcTask`: null");
        Preconditions.checkArgument(distance > 0, "Argument `distance` should be positive, but found: %d", distance);

        Map<String, List<String>> columnsMap = new HashMap<>();
        columnsMap.put(TASK_RELATION_MODEL_NAME, taskRelationCols);

        int remainingIteration = distance;
        Set<Long> resultTaskIds = new HashSet<>();
        List<Long> inClauseIdSet = Lists.newArrayList(srcTask.getId());
        String whereCol = queryDirection == DependencyDirection.UPSTREAM ? "downstream_task_id" : "upstream_task_id";
        if (includeSelf) {
            resultTaskIds.add(srcTask.getId());
        }

        while ((remainingIteration > 0) && (!inClauseIdSet.isEmpty())) {
            String idsFieldsPlaceholder = "(" + inClauseIdSet.stream().map(id -> "?")
                    .collect(Collectors.joining(", ")) + ")";

            String sql = DefaultSQLBuilder.newBuilder()
                    .columns(columnsMap)
                    .from(TASK_RELATION_TABLE_NAME, TASK_RELATION_MODEL_NAME)
                    .where(TASK_RELATION_MODEL_NAME + "." + whereCol + " IN " + idsFieldsPlaceholder)
                    .autoAliasColumns()
                    .asPrepared()
                    .getSQL();
            List<TaskDependency> results = dbOperator.fetchAll(
                    sql,
                    TaskDependencyMapper.getInstance(functionProvider),
                    inClauseIdSet.toArray()
            );
            for (TaskDependency dependency : results) {
                resultTaskIds.add((queryDirection == DependencyDirection.UPSTREAM) ?
                        dependency.getUpstreamTaskId() : dependency.getDownstreamTaskId()
                );
            }
            inClauseIdSet = results.stream().map((queryDirection == DependencyDirection.UPSTREAM) ?
                    TaskDependency::getUpstreamTaskId : TaskDependency::getDownstreamTaskId)
                    .collect(Collectors.toList());
            remainingIteration -= 1;
        }

        return resultTaskIds;
    }

    private List<Task> fetchTasksByIds(Collection<Long> taskIds) {
        Preconditions.checkNotNull(taskIds, "Invalid argument `taskIds`: null");
        if (taskIds.isEmpty()) {
            return new ArrayList<>();
        }

        Map<String, List<String>> columnsMap = new HashMap<>();
        columnsMap.put(TASK_MODEL_NAME, taskCols);

        String idsFieldsPlaceholder = "(" + taskIds.stream().map(id -> "?")
                .collect(Collectors.joining(", ")) + ")";
        String sql = DefaultSQLBuilder.newBuilder()
                .columns(columnsMap)
                .from(TASK_TABLE_NAME, TASK_MODEL_NAME)
                .where(TASK_MODEL_NAME + ".id IN " + idsFieldsPlaceholder)
                .autoAliasColumns()
                .asPrepared()
                .getSQL();

        return fetchTasksJoiningDependenciesAndTags(sql, taskIds.toArray());
    }

    private List<Task> fetchTasksJoiningDependenciesAndTags(String preparedSql, Object... params) {
        List<Task> plainTasks = dbOperator.fetchAll(preparedSql, TaskMapper.INSTANCE, params);

        // Retrieve all task tags, and relations whose downstream ids are involved in result tasks
        List<Long> plainTaskIds = plainTasks.stream().map(Task::getId).collect(Collectors.toList());
        Map<Long, List<TaskDependency>> dependenciesMap = fetchAllRelationsFromDownstreamTaskIds(plainTaskIds);
        Map<Long, List<Tag>> tagsMap = fetchTaskTagsByTaskIds(plainTaskIds);
        List<Task> sortTasks = topoSort(dependenciesMap, plainTasks);

        // re-construct all tasks with full properties
        return sortTasks.stream()
                .map(t -> t.cloneBuilder()
                        .withDependencies(dependenciesMap.get(t.getId()))
                        .withTags(tagsMap.containsKey(t.getId()) ? tagsMap.get(t.getId()) : new ArrayList<>())
                        .build())
                .collect(Collectors.toList());
    }

    private List<Task> topoSort(Map<Long, List<TaskDependency>> dependenciesMap, List<Task> plainTasks) {
        Map<Long, List<Long>> upstreamDependencyMap = coverUpstreamDependencyMap(dependenciesMap);
        List<Task> sortTasks = new ArrayList<>();
        Queue<Task> queue = new LinkedList<>();
        int size = plainTasks.size();
        do {
            if (!queue.isEmpty()) {
                Task sortedTask = queue.poll();
                upstreamDependencyMap.remove(sortedTask.getId());
                sortTasks.add(sortedTask);
            }
            Iterator<Task> iterator = plainTasks.listIterator();
            while (iterator.hasNext()) {
                Task task = iterator.next();
                if (!taskHasUpstream(task, upstreamDependencyMap)) {
                    queue.offer(task);
                    iterator.remove();
                }
            }
        } while (!queue.isEmpty());
        if (sortTasks.size() != size) {
            List<Long> cycleDependTasks = plainTasks.stream().map(Task::getId).collect(Collectors.toList());
            logger.error("tasks:{},has cycle dependencies", cycleDependTasks);
            throw ExceptionUtils.wrapIfChecked(new Exception("has cycle in task dependencies"));
        }
        return sortTasks;
    }

    private Boolean taskHasUpstream(Task task, Map<Long, List<Long>> upstreamDependencyMap) {
        boolean hasUpstream = false;
        List<Long> taskDependencyList = upstreamDependencyMap.get(task.getId());
        for (Long taskId : taskDependencyList) {
            if (upstreamDependencyMap.containsKey(taskId)) {
                hasUpstream = true;
                break;
            }
        }
        return hasUpstream;
    }

    private Map<Long, List<Long>> coverUpstreamDependencyMap(Map<Long, List<TaskDependency>> dependenciesMap) {
        Map<Long, List<Long>> upstreamDependencyMap = new HashMap<>();
        for (Map.Entry<Long, List<TaskDependency>> entry : dependenciesMap.entrySet()) {
            List<Long> upstreamDependency = entry.getValue().stream().
                    map(TaskDependency::getUpstreamTaskId).collect(Collectors.toList());
            upstreamDependencyMap.put(entry.getKey(), upstreamDependency);
        }
        return upstreamDependencyMap;
    }

    public List<Tag> fetchTaskTagsByTaskId(Long taskId) {
        Map<Long, List<Tag>> results = fetchTaskTagsByTaskIds(Lists.newArrayList(taskId));
        return results.containsKey(taskId) ? results.get(taskId) : new ArrayList<>();
    }

    public Pair<String, List<Object>> generateFilterByTagSQLClause(List<Tag> tags) {
        List<String> whereSubClauses = new ArrayList<>();
        List<Object> params = new ArrayList<>();

        tags.forEach(tag -> {
            whereSubClauses.add("(" + TASK_TAGS_MODEL_NAME + ".tag_key = ? AND " + TASK_TAGS_MODEL_NAME + ".tag_value = ?)");
            params.add(tag.getKey());
            params.add(tag.getValue());
        });
        String whereClause = StringUtils.joinWith(" OR ", whereSubClauses.toArray());

        String taskIdColumn = TASK_TAGS_MODEL_NAME + ".task_id";
        String sql = DefaultSQLBuilder.newBuilder()
                .select(taskIdColumn)
                .from(TASK_TAGS_TABLE_NAME, TASK_TAGS_MODEL_NAME)
                .where(whereClause)
                .groupBy(taskIdColumn)
                .having("count(1) = ?")
                .getSQL();

        // should match all tags as required
        params.add(tags.size());
        return Pair.of(sql, params);
    }

    /**
     * Fetch all tags in `taskid - [dependencies]` hashmap by given task ids
     *
     * @param taskIds list of task ids
     * @return
     */
    private Map<Long, List<Tag>> fetchTaskTagsByTaskIds(List<Long> taskIds) {
        if (taskIds.isEmpty()) {
            return new HashMap<>();
        }

        Map<String, List<String>> taskRelationColumnsMap = new HashMap<>();
        taskRelationColumnsMap.put(TASK_TAGS_MODEL_NAME, taskTagCols);
        String inClausePlaceholders = "(" + StringUtils.repeat("?", ",", taskIds.size()) + ")";
        String sql = DefaultSQLBuilder.newBuilder()
                .columns(taskRelationColumnsMap)
                .from(TASK_TAGS_TABLE_NAME, TASK_TAGS_MODEL_NAME)
                .autoAliasColumns()
                .where(TASK_TAGS_MODEL_NAME + ".task_id IN " + inClausePlaceholders)
                .getSQL();
        List<TagVO> taskTagVOs = dbOperator.fetchAll(sql, TaskTagMapper.INSTANCE, taskIds.toArray());
        Map<Long, List<Tag>> resultSet = new HashMap<>();
        taskTagVOs.forEach(taskTagVO -> {
            if (!resultSet.containsKey(taskTagVO.getTaskId())) {
                resultSet.put(taskTagVO.getTaskId(), new ArrayList<>());
            }
            resultSet.get(taskTagVO.getTaskId()).add(new Tag(taskTagVO.getKey(), taskTagVO.getValue()));
        });
        return resultSet;
    }

    /**
     * Fetch all relations in a `id - [dependencies]` hashmap whose downstream task ID is included in the given list
     *
     * @param taskIds list of downstream task IDs
     * @return
     */
    private Map<Long, List<TaskDependency>> fetchAllRelationsFromDownstreamTaskIds(List<Long> taskIds) {
        if (taskIds.isEmpty()) {
            return new HashMap<>();
        }

        Map<Long, List<TaskDependency>> taskIdToDependenciesMap = new HashMap<>();
        taskIds.forEach(taskId -> {
            taskIdToDependenciesMap.put(taskId, new ArrayList<>());
        });
        String idsFieldsPlaceholder = "(" + taskIds.stream().map(id -> "?")
                .collect(Collectors.joining(", ")) + ")";
        Map<String, List<String>> taskRelationColumnsMap = new HashMap<>();
        taskRelationColumnsMap.put(TASK_RELATION_MODEL_NAME, taskRelationCols);
        String sql = DefaultSQLBuilder.newBuilder()
                .columns(taskRelationColumnsMap)
                .from(TASK_RELATION_TABLE_NAME, TASK_RELATION_MODEL_NAME)
                .autoAliasColumns()
                .where(TASK_RELATION_MODEL_NAME + ".downstream_task_id IN " + idsFieldsPlaceholder)
                .orderBy(TASK_RELATION_MODEL_NAME + ".upstream_task_id ASC")
                .asPrepared()
                .getSQL();
        List<TaskDependency> allDeps = dbOperator.fetchAll(sql, TaskDependencyMapper.getInstance(functionProvider), taskIds.toArray());
        allDeps.forEach(dep -> {
            List<TaskDependency> dependencyList = taskIdToDependenciesMap.get(dep.getDownstreamTaskId());
            dependencyList.add(dep);
            taskIdToDependenciesMap.put(dep.getDownstreamTaskId(), dependencyList);
        });
        return taskIdToDependenciesMap;
    }

    private boolean deleteTickTaskMappingRecord(Long taskId) {
        String sql = DefaultSQLBuilder.newBuilder()
                .delete()
                .from(TICK_TASK_MAPPING_TABLE_NAME)
                .where(TASK_ID_QUERY)
                .getSQL();
        int affectedRows = dbOperator.update(sql, taskId);
        return affectedRows > 0;
    }

    private boolean deleteTaskRelations(Long taskId) {
        String sql = DefaultSQLBuilder.newBuilder()
                .delete()
                .from(TASK_RELATION_TABLE_NAME)
                .where("(upstream_task_id = ?) OR (downstream_task_id = ?)")
                .getSQL();
        int affectedRows = dbOperator.update(sql, taskId, taskId);
        return affectedRows > 0;
    }

    private String getSelectSQL(String whereClause) {
        Map<String, List<String>> columnsMap = new HashMap<>();
        columnsMap.put(TASK_MODEL_NAME, taskCols);
        SQLBuilder builder = DefaultSQLBuilder.newBuilder()
                .columns(columnsMap)
                .from(TASK_TABLE_NAME, TASK_MODEL_NAME)
                .autoAliasColumns();

        if (StringUtils.isNotBlank(whereClause)) {
            builder.where(whereClause);
        }

        return builder.getSQL();
    }

    /**
     * Generator where clause with parameter placeholders and list of params
     *
     * @param filters task search filter instance
     * @return Pair of (where clause, list of parameter objects)
     */
    private Pair<String, List<Object>> generateWhereClauseAndParamsFromFilter(TaskSearchFilter filters) {
        Preconditions.checkNotNull(filters, "Invalid argument `filters`: null");

        String whereClause = "(1 = 1)";
        List<Object> params = new ArrayList<>();

        boolean filterHasKeyword = StringUtils.isNotBlank(filters.getName());
        boolean filterHasTags = Objects.nonNull(filters.getTags()) && (!filters.getTags().isEmpty());

        if (filterHasKeyword) {
            whereClause = "(" + TASK_MODEL_NAME + ".name LIKE CONCAT('%', CAST(? AS TEXT), '%'))";
            params.add(filters.getName());
        }

        if (filterHasTags) {
            Pair<String, List<Object>> sqlAndParams = generateFilterByTagSQLClause(filters.getTags());
            String filterByTagsSql = sqlAndParams.getLeft();
            List<Object> filterByTagsSqlParams = sqlAndParams.getRight();

            whereClause += " AND (" + TASK_MODEL_NAME + ".id IN (" + filterByTagsSql + "))";
            params.addAll(filterByTagsSqlParams);
        }

        return Pair.of(whereClause, params);
    }

    /**
     * Get paginated task records that matches the constraints by given filter.
     *
     * @param filters task search filter instance
     * @return list of filtered and paginated task records
     */
    public List<Task> fetchWithFilters(TaskSearchFilter filters) {
        Preconditions.checkArgument(Objects.nonNull(filters.getPageNum()) && filters.getPageNum() > 0, "Invalid page num: %d", filters.getPageNum());
        Preconditions.checkArgument(Objects.nonNull(filters.getPageSize()) && filters.getPageSize() > 0, "Invalid page size: %d", filters.getPageSize());

        Integer pageNum = filters.getPageNum();
        Integer pageSize = filters.getPageSize();
        Integer offset = (pageNum - 1) * pageSize;

        Map<String, List<String>> taskRelationColumnsMap = new HashMap<>();
        taskRelationColumnsMap.put(TASK_MODEL_NAME, taskCols);

        Pair<String, List<Object>> whereClauseAndParams = generateWhereClauseAndParamsFromFilter(filters);
        String whereClause = whereClauseAndParams.getLeft();
        List<Object> params = whereClauseAndParams.getRight();

        String sql = DefaultSQLBuilder.newBuilder()
                .columns(taskRelationColumnsMap)
                .autoAliasColumns()
                .from(TASK_TABLE_NAME, TASK_MODEL_NAME)
                .where(whereClause)
                .limit(pageSize)
                .offset(offset)
                .asPrepared()
                .getSQL();

        Collections.addAll(params, pageSize, offset);

        return fetchTasksJoiningDependenciesAndTags(sql, params.toArray());
    }

    /**
     * Get total count of task records
     *
     * @return total count of records
     */
    public Integer fetchTotalCount() {
        return fetchTotalCountWithFilters(TaskSearchFilter.newBuilder().build());
    }

    /**
     * Get total count of task records that matches the constraints by given filter. Pagination is not affected.
     *
     * @param filters task search filter instance
     * @return total count of records that matches the constraints by given filter
     */
    public Integer fetchTotalCountWithFilters(TaskSearchFilter filters) {
        Pair<String, List<Object>> whereClauseAndParams = generateWhereClauseAndParamsFromFilter(filters);
        String whereClause = whereClauseAndParams.getLeft();
        List<Object> params = whereClauseAndParams.getRight();

        String sql = DefaultSQLBuilder.newBuilder()
                .select("COUNT(*)")
                .from(TASK_TABLE_NAME, TASK_MODEL_NAME)
                .where(whereClause)
                .asPrepared()
                .getSQL();

        return dbOperator.fetchOne(sql, rs -> rs.getInt(1), params.toArray());
    }

    public List<Task> fetchByOperatorId(Long operatorId) {
        Preconditions.checkNotNull(operatorId, "Invalid argument `operatorId`: null");
        String sql = getSelectSQL(TASK_MODEL_NAME + ".operator_id = ?");
        return dbOperator.fetchAll(sql, TaskMapper.INSTANCE, operatorId);
    }

    public Optional<Task> fetchById(Long taskId) {
        String sql = getSelectSQL(TASK_MODEL_NAME + ".id = ?");
        Task task = dbOperator.fetchOne(sql, TaskMapper.INSTANCE, taskId);
        if (Objects.isNull(task)) {
            return Optional.empty();
        }
        // else
        Map<String, List<String>> taskRelationColumnsMap = new HashMap<>();
        taskRelationColumnsMap.put(TASK_RELATION_MODEL_NAME, taskRelationCols);
        String dependenciesQuerySQL = DefaultSQLBuilder.newBuilder()
                .columns(taskRelationColumnsMap)
                .from(TASK_RELATION_TABLE_NAME, TASK_RELATION_MODEL_NAME)
                .autoAliasColumns()
                .where(TASK_RELATION_MODEL_NAME + ".downstream_task_id = ?")
                .orderBy(TASK_RELATION_MODEL_NAME + ".upstream_task_id ASC")
                .asPrepared()
                .getSQL();

        List<TaskDependency> dependencies = dbOperator.fetchAll(
                dependenciesQuerySQL,
                TaskDependencyMapper.getInstance(functionProvider),
                taskId
        );
        List<Tag> tags = fetchTaskTagsByTaskId(taskId);

        return Optional.of(task.cloneBuilder().withDependencies(dependencies).withTags(tags).build());
    }

    public Map<Long, Optional<Task>> fetchByIds(Collection<Long> taskIds) {
        if (taskIds.isEmpty()) {
            return Collections.emptyMap();
        }

        String idList = taskIds.stream()
                .map(Object::toString)
                .collect(Collectors.joining(","));

        String sql = DefaultSQLBuilder.newBuilder()
                .select(getSelectSQL(null))
                .where("id IN (" + idList + ")")
                .getSQL();

        List<Task> fetched = fetchTasksJoiningDependenciesAndTags(sql);

        Map<Long, Optional<Task>> result = fetched.stream()
                .collect(Collectors.toMap(Task::getId, Optional::of));

        for (Long taskId : taskIds) {
            if (!result.containsKey(taskId)) {
                result.put(taskId, Optional.empty());
            }
        }

        return result;
    }

    public Optional<Task> fetchByName(String name) {
        Preconditions.checkNotNull(name, "Invalid parameter `name`: found null object");
        String sql = getSelectSQL(TASK_MODEL_NAME + ".name = ?");
        Task task = dbOperator.fetchOne(sql, TaskMapper.INSTANCE, name);
        return Optional.ofNullable(task);
    }

    public void create(Task task) {
        /*
         * Creating a task consists of following steps:
         * 1. Insert task record into database
         * 2. Insert a tick-task mapping record according to schedule config
         * 3. For each dependency, insert records into table `kun_wf_task_relations`
         * 4. For each tag, upsert records into table `kun_wf_task_tags`
         * Note: if any of the steps above failed, the entire insertion operation should be aborted and reverted.
         * */
        List<String> tableColumns = new ImmutableList.Builder<String>()
                .addAll(taskCols)
                .build();

        String sql = DefaultSQLBuilder.newBuilder()
                .insert(tableColumns.toArray(new String[0]))
                .into(TASK_TABLE_NAME)
                .asPrepared()
                .getSQL();

        dbOperator.transaction(() -> {
            dbOperator.update(
                    sql,
                    task.getId(),
                    task.getName(),
                    task.getDescription(),
                    task.getOperatorId(),
                    JSONUtils.toJsonString(task.getConfig()),
                    JSONUtils.toJsonString(task.getScheduleConf()),
                    task.getQueueName(),
                    task.getPriority()
            );
            insertTickTaskRecordByScheduleConf(task.getId(), task.getScheduleConf());
            // Update tags
            updateTaskTags(task, task.getTags());
            // Update upstream dependencies
            updateTaskUpstreamDependencies(task, task.getDependencies());
            return null;
        });
    }

    public boolean update(Task task) {
        List<String> tableColumns = new ImmutableList.Builder<String>()
                .addAll(taskCols)
                .build();

        String sql = DefaultSQLBuilder.newBuilder()
                .update(TASK_TABLE_NAME)
                .set(tableColumns.toArray(new String[0]))
                .where("id = ?")
                .asPrepared()
                .getSQL();

        int affectedRows = dbOperator.transaction(() -> {
            int updatedRows = dbOperator.update(
                    sql,
                    task.getId(),
                    task.getName(),
                    task.getDescription(),
                    task.getOperatorId(),
                    JSONUtils.toJsonString(task.getConfig()),
                    JSONUtils.toJsonString(task.getScheduleConf()),
                    task.getQueueName(),
                    task.getPriority(),
                    task.getId()
            );

            // remove existing task mappings, if any
            deleteTickTaskMappingRecord(task.getId());
            // and re-insert by updated schedule configuration
            insertTickTaskRecordByScheduleConf(task.getId(), task.getScheduleConf());
            // update tags
            updateTaskTags(task, task.getTags());
            updateTaskUpstreamDependencies(task, task.getDependencies());
            return updatedRows;
        });
        return affectedRows > 0;
    }

    public boolean deleteById(Long taskId) {
        return dbOperator.transaction(() -> {
            String sql = DefaultSQLBuilder.newBuilder()
                    .delete()
                    .from(TASK_TABLE_NAME)
                    .where("id = ?")
                    .getSQL();
            int affectedRows = dbOperator.update(sql, taskId);
            // remove existing task mappings, if any
            deleteTickTaskMappingRecord(taskId);
            // remove existing task relations, if any
            deleteTaskRelations(taskId);
            return affectedRows > 0;
        });
    }

    /**
     * update task scheduled_tick only if next execution time is larger than currentTick.
     * delete tasks which is never called again
     *
     * @param currentTick
     * @param tasks:      tasks to be updated or deleted
     */
    public void updateTasksNextExecutionTick(Tick currentTick, List<Task> tasks) {

        if (tasks.isEmpty()) {
            return;
        }

        List<Object[]> taskToBeDeleted = new ArrayList<>();
        List<Object[]> taskToBeUpdated = new ArrayList<>();
        for (Task task : tasks) {
            ScheduleConf scheduleConf = task.getScheduleConf();
            Long taskId = task.getId();
            switch (scheduleConf.getType()) {
                case ONESHOT:
                    // for ONESHOT type, should delete tick task after invocation once
                    logger.debug("Delete oneshot task {} schedule tick at {}", taskId, currentTick);
                    taskToBeDeleted.add(new Object[]{taskId});
                    break;
                case SCHEDULED:
                    String cronExpression = scheduleConf.getCronExpr();
                    Cron cron = CronUtils.convertStringToCron(cronExpression);
                    Optional<OffsetDateTime> nextExecutionTimeOptional = CronUtils.getNextExecutionTime(cron,
                            currentTick.toOffsetDateTime());

                    if (nextExecutionTimeOptional.isPresent()) {
                        Tick nextTick = new Tick(nextExecutionTimeOptional.get());
                        logger.debug("Update scheduled task {} next schedule tick to {} at current {}", taskId, nextTick, currentTick);
                        taskToBeUpdated.add(new Object[]{nextTick.toString(), taskId});
                    } else {
                        logger.debug("Delete scheduled task {} schedule tick at {}", taskId, currentTick);
                        taskToBeDeleted.add(new Object[]{taskId});
                    }
                    break;
                default:
                    throw new RuntimeException(String.format("invalid task %s schedule type %s ", taskId, scheduleConf.getType()));
            }
        }

        String tickTaskUpdateSql = DefaultSQLBuilder.newBuilder()
                .update(TICK_TASK_MAPPING_TABLE_NAME)
                .set("scheduled_tick")
                .where(TASK_ID_QUERY)
                .asPrepared()
                .getSQL();
        String deleteTickTask = DefaultSQLBuilder.newBuilder()
                .delete()
                .from(TICK_TASK_MAPPING_TABLE_NAME)
                .where(TASK_ID_QUERY)
                .asPrepared()
                .getSQL();


        dbOperator.batch(tickTaskUpdateSql, taskToBeUpdated.stream().toArray(Object[][]::new));
        dbOperator.batch(deleteTickTask, taskToBeDeleted.stream().toArray(Object[][]::new));

    }

    public Optional<Tick> fetchNextExecutionTickByTaskId(Long taskId) {
        String sql = DefaultSQLBuilder.newBuilder()
                .select("scheduled_tick")
                .from(TICK_TASK_MAPPING_TABLE_NAME)
                .where(TASK_ID_QUERY)
                .orderBy("scheduled_tick ASC")
                .limit(1)
                .asPrepared().getSQL();
        String nextExecutionTimeString = dbOperator.fetchOne(
                sql,
                rs -> rs.getString("scheduled_tick"),
                taskId, 1
        );
        if (Objects.nonNull(nextExecutionTimeString)) {
            return Optional.of(new Tick(nextExecutionTimeString));
        } else {
            return Optional.empty();
        }
    }

    public List<Task> fetchScheduledTaskAtTick(Tick tick) {
        String sql = DefaultSQLBuilder.newBuilder()
                .select(getSelectSQL(null))
                .join("INNER", TICK_TASK_MAPPING_TABLE_NAME, TICK_TASK_MAPPING_TABLE_ALIAS)
                .on(TASK_MODEL_NAME + ".id = " + TICK_TASK_MAPPING_TABLE_ALIAS + ".task_id")
                .where(TICK_TASK_MAPPING_TABLE_ALIAS + ".scheduled_tick <= ?")
                .getSQL();
        return fetchTasksJoiningDependenciesAndTags(sql, tick.toString());
    }

    /**
     * Fetch and return list of one-hop upstream tasks with given source task (source task not included)
     *
     * @param srcTask source task
     * @return list of upstream tasks
     */
    public List<Task> fetchUpstreamTasks(Task srcTask) {
        return fetchUpstreamTasks(srcTask, 1, false);
    }

    /**
     * Fetch and return upstream tasks within maximum distance from given source task (source task not included)
     *
     * @param srcTask  source task
     * @param distance max distance from source task, required to be positive
     * @return list of upstream tasks
     * @throws IllegalArgumentException when distance is not positive
     */
    public List<Task> fetchUpstreamTasks(Task srcTask, int distance) {
        return fetchUpstreamTasks(srcTask, distance, false);
    }

    /**
     * Fetch and return upstream tasks within maximum distance from given source task
     *
     * @param srcTask     source task
     * @param distance    max distance from source task, required to be positive
     * @param includeSelf whether source task should be included
     * @return list of upstream tasks
     * @throws IllegalArgumentException when distance is not positive
     */
    public List<Task> fetchUpstreamTasks(Task srcTask, int distance, boolean includeSelf) {
        Preconditions.checkNotNull(srcTask, "Invalid argument `srcTask`: null");
        Preconditions.checkArgument(distance > 0, "Argument `distance` should be positive, but found: %d", distance);

        Set<Long> upstreamTaskIds = retrieveTaskIdsWithinDependencyDistance(srcTask, distance, DependencyDirection.UPSTREAM, includeSelf);
        return fetchTasksByIds(upstreamTaskIds);
    }

    /**
     * Fetch and return list of one-hop downstream tasks with given source task (source task not included)
     *
     * @param srcTask source task
     * @return list of downstream tasks
     */
    public List<Task> fetchDownstreamTasks(Task srcTask) {
        return fetchDownstreamTasks(srcTask, 1, false);
    }

    /**
     * Fetch and return downstream tasks within maximum distance from given source task (source task not included)
     *
     * @param srcTask  source task
     * @param distance max distance from source task, required to be positive
     * @return list of downstream tasks
     * @throws IllegalArgumentException when distance is not positive
     */
    public List<Task> fetchDownstreamTasks(Task srcTask, int distance) {
        return fetchDownstreamTasks(srcTask, distance, false);
    }

    /**
     * Fetch and return downstream tasks within maximum distance from given source task
     *
     * @param srcTask     source task
     * @param distance    max distance from source task, required to be positive
     * @param includeSelf whether source task should be included
     * @return list of downstream tasks
     * @throws IllegalArgumentException when distance is illegal
     */
    public List<Task> fetchDownstreamTasks(Task srcTask, int distance, boolean includeSelf) {
        Preconditions.checkNotNull(srcTask, "Invalid argument `srcTask`: null");
        Preconditions.checkArgument(distance > 0, "Argument `distance` should be positive, but found: %d", distance);

        Set<Long> downstreamTaskIds = retrieveTaskIdsWithinDependencyDistance(srcTask, distance, DependencyDirection.DOWNSTREAM, includeSelf);
        return fetchTasksByIds(downstreamTaskIds);
    }


    /**
     *
     * @param taskId
     * @param taskIds upstream taskIds
     * @return
     */
    public List<Long> getCycleDependencies(Long taskId, List<Long> taskIds) {
        String filterTaskId = taskIds.stream().map(x -> "?").collect(Collectors.joining(","));

        String sql = "WITH RECURSIVE checkcycle(task_id,path,cycle) as \n" +
                "(SELECT upstream_task_id,ARRAY[downstream_task_id,upstream_task_id],FALSE from kun_wf_task_relations \n" +
                "WHERE downstream_task_id in " + filterTaskId + " \n" +
                "UNION \n" +
                "SELECT kw.upstream_task_id,path || kw.upstream_task_id,kw.upstream_task_id != ? FROM checkcycle as c \n" +
                "INNER JOIN kun_wf_task_relations as kw \n" +
                "ON c.task_id = kw.downstream_task_id\n" +
                "WHERE NOT c.cycle\n" +
                ") SELECT task_id,path,cycle FROM checkcycle WHERE cycle = TRUE;";
        taskIds.add(taskId);
        List<Object> result = dbOperator.fetchAll(sql, rs -> rs.getArray(1), taskIds);
        return result.stream().map(x -> Long.valueOf(x.toString())).collect(Collectors.toList());
    }

    public static class TaskMapper implements ResultSetMapper<Task> {
        public static final TaskDao.TaskMapper INSTANCE = new TaskMapper();

        @Override
        public Task map(ResultSet rs) throws SQLException {
            return Task.newBuilder()
                    .withId(rs.getLong(TASK_MODEL_NAME + "_id"))
                    .withName(rs.getString(TASK_MODEL_NAME + "_name"))
                    .withDescription(rs.getString(TASK_MODEL_NAME + "_description"))
                    .withOperatorId(rs.getLong(TASK_MODEL_NAME + "_operator_id"))
                    .withConfig(JSONUtils.jsonToObject(rs.getString(TASK_MODEL_NAME + "_config"), Config.class))
                    .withScheduleConf(JSONUtils.jsonToObject(rs.getString(TASK_MODEL_NAME + "_schedule"), ScheduleConf.class))
                    .withDependencies(new ArrayList<>())
                    .withTags(new ArrayList<>())
                    .withQueueName(rs.getString(TASK_MODEL_NAME + "_queue_name"))
                    .withPriority(rs.getInt(TASK_MODEL_NAME + "_priority"))
                    .build();
        }
    }

    public static class TaskDependencyMapper implements ResultSetMapper<TaskDependency> {
        private final TaskDependencyFunctionProvider functionProvider;

        private static TaskDependencyMapper instance;

        private TaskDependencyMapper(TaskDependencyFunctionProvider functionProvider) {
            this.functionProvider = functionProvider;
        }

        public static TaskDependencyMapper getInstance(TaskDependencyFunctionProvider functionProvider) {
            if (instance == null) {
                instance = new TaskDependencyMapper(functionProvider);
            }
            return instance;
        }

        @Override
        public TaskDependency map(ResultSet rs) throws SQLException {
            return new TaskDependency(
                    rs.getLong(TASK_RELATION_MODEL_NAME + "_upstream_task_id"),
                    rs.getLong(TASK_RELATION_MODEL_NAME + "_downstream_task_id"),
                    functionProvider.from(rs.getString(TASK_RELATION_MODEL_NAME + "_dependency_function")),
                    DependencyLevel.resolve(rs.getString(TASK_RELATION_MODEL_NAME + "_dependency_level"))
            );
        }
    }

    public static class TaskTagMapper implements ResultSetMapper<TagVO> {
        public static final TaskTagMapper INSTANCE = new TaskTagMapper();

        @Override
        public TagVO map(ResultSet rs) throws SQLException {
            return new TagVO(
                    rs.getLong(TASK_TAGS_MODEL_NAME + "_task_id"),
                    rs.getString(TASK_TAGS_MODEL_NAME + "_tag_key"),
                    rs.getString(TASK_TAGS_MODEL_NAME + "_tag_value")
            );
        }
    }
}
