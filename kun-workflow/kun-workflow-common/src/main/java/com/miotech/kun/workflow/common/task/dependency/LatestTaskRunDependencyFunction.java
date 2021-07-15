package com.miotech.kun.workflow.common.task.dependency;

import com.google.common.collect.Lists;
import com.miotech.kun.workflow.common.taskrun.dao.TaskRunDao;
import com.miotech.kun.workflow.core.model.common.Tick;
import com.miotech.kun.workflow.core.model.task.DependencyFunction;
import com.miotech.kun.workflow.core.model.task.Task;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;

//默认选择一天以内的最近一次
@Singleton
public class LatestTaskRunDependencyFunction implements DependencyFunction {
    private final String functionType = "latestTaskRun";
    private final Logger logger = LoggerFactory.getLogger(LatestTaskRunDependencyFunction.class);

    // make this field 'transient' to avoid serialization failure of matcher sameBeanAs() in testing
    private transient final TaskRunDao taskRunDao;

    @Inject
    public LatestTaskRunDependencyFunction(TaskRunDao taskRunDao) {
        this.taskRunDao = taskRunDao;
    }

    @Override
    public List<TaskRun> resolveDependency(Task self, Long upstreamTaskId, Tick tick, List<TaskRun> others) {
        for (TaskRun otherTaskRun : others) {
            if (otherTaskRun.getTask().getId().equals(upstreamTaskId)) {
                return Lists.newArrayList(otherTaskRun);
            }
        }

        TaskRun upstreamTaskRun = taskRunDao.fetchLatestTaskRun(upstreamTaskId);
        if (upstreamTaskRun == null) {
            logger.error("upstreamTask never start, taskId = {},upstream taskId = {}", self.getId(), upstreamTaskId);
            return Lists.newArrayList();
        }
        return Lists.newArrayList(upstreamTaskRun);
    }

    @Override
    public String toFunctionType() {
        return functionType;
    }
}
