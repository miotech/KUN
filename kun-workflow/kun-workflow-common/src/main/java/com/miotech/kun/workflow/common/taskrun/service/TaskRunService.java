package com.miotech.kun.workflow.common.taskrun.service;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.miotech.kun.workflow.common.taskrun.dao.TaskRunDao;
import com.miotech.kun.workflow.core.model.taskrun.TaskAttempt;
import com.miotech.kun.workflow.core.model.taskrun.TaskRun;
import com.miotech.kun.workflow.core.model.vo.TaskRunVO;
import java.util.List;
import java.util.Optional;

@Singleton
public class TaskRunService {

    @Inject
    private TaskRunDao taskRunDao;


    public Optional<TaskRunVO> getTaskRunDetail(Long taskRunId) {
        Optional<TaskRun> taskRun = taskRunDao.fetchById(taskRunId);
        return taskRun.map(this::convertToVO);
    }

    public TaskRunVO convertToVO(TaskRun taskRun) {
        List<TaskAttempt> attempts = taskRunDao.fetchAttemptsByTaskRunId(taskRun.getId());

        return TaskRunVO.newBuilder()
                .withTask(taskRun.getTask())
                .withId(taskRun.getId())
                .withScheduledTick(taskRun.getScheduledTick())
                .withStatus(taskRun.getStatus())
                .withInlets(taskRun.getInlets())
                .withOutlets(taskRun.getOutlets())
                .withDependencyTaskRunIds(taskRun.getDependentTaskRunIds())
                .withStartAt(taskRun.getStartAt())
                .withEndAt(taskRun.getEndAt())
                .withVariables(taskRun.getVariables())
                .withAttempts(attempts)
                .build();
    }
}
