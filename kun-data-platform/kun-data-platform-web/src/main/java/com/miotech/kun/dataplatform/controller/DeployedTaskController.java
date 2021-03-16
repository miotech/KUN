package com.miotech.kun.dataplatform.controller;

import com.miotech.kun.common.model.RequestResult;
import com.miotech.kun.dataplatform.common.deploy.service.DeployedTaskService;
import com.miotech.kun.dataplatform.common.deploy.vo.*;
import com.miotech.kun.dataplatform.common.taskdefinition.vo.TaskRunLogVO;
import com.miotech.kun.dataplatform.model.deploy.DeployedTask;
import com.miotech.kun.workflow.client.model.PaginationResult;
import com.miotech.kun.workflow.client.model.TaskRun;
import com.miotech.kun.workflow.client.model.TaskRunDAG;
import com.miotech.kun.workflow.core.model.taskrun.TaskRunStatus;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.*;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@RestController
@RequestMapping("/")
@Api(tags ="DeployedTask")
@Slf4j
public class DeployedTaskController {

    @Autowired
    private DeployedTaskService deployedTaskService;

    @GetMapping("/deployed-tasks/{id}")
    @ApiOperation("Get DeployedTask")
    public RequestResult<DeployedTaskVO> getDeployedTask(@PathVariable Long id) {
        DeployedTask deployedTask = deployedTaskService.find(id);
        return RequestResult.success(deployedTaskService.convertVO(deployedTask));
    }

    @GetMapping("/deployed-tasks")
    @ApiOperation("Search DeployedTasks")
    public RequestResult<PaginationResult<DeployedTaskWithRunVO>> searchDeploys(
            @RequestParam(defaultValue = "1") int pageNum,
            @RequestParam(defaultValue = "100") int pageSize,
            @RequestParam(required = false) List<Long> ownerIds,
            @RequestParam(required = false) List<Long> definitionIds,
            @RequestParam(required = false) List<Long> workflowTaskIds,
            @RequestParam(required = false) String name,
            @RequestParam(required = false) String taskTemplateName
    ) {
        DeployedTaskSearchRequest deploySearchRequest = new DeployedTaskSearchRequest(
                pageSize,
                pageNum,
                definitionIds,
                ownerIds,
                taskTemplateName,
                name,
                workflowTaskIds
        );
        PaginationResult<DeployedTask> deploys = deployedTaskService.search(deploySearchRequest);
        PaginationResult<DeployedTaskWithRunVO> result = new PaginationResult<>(
                deploys.getPageSize(),
                deploys.getPageNum(),
                deploys.getTotalCount(),
                deployedTaskService.convertToListVOs(deploys.getRecords())
        );
        return RequestResult.success(result);
    }

    @GetMapping("/deployed-tasks/{id}/dag")
    @ApiOperation("Get dag of deployed task")
    public RequestResult<DeployedTaskDAG> getDeployedTaskDAG(@PathVariable Long id,
                                                                   @RequestParam(defaultValue =  "1") int upstreamLevel,
                                                                   @RequestParam(defaultValue =  "1") int downstreamLevel) {
        return RequestResult.success(deployedTaskService.getDeployedTaskDag(id, upstreamLevel, downstreamLevel));
    }

    @GetMapping("/deployed-tasks/{id}/taskruns")
    @ApiOperation("Search taskruns of deployedtask")
    public RequestResult<PaginationResult<TaskRun>> getDeployTaskRuns(
            @PathVariable Long id,
            @RequestParam(defaultValue = "1") int pageNum,
            @RequestParam(defaultValue = "100") int pageSize,
            @RequestParam(required = false) String status,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime startTime,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime endTime
    ) {
        ScheduledTaskRunSearchRequest deploySearchRequest = new ScheduledTaskRunSearchRequest(
                pageSize,
                pageNum,
                Optional.empty(),
                Collections.singletonList(id),
                null,
                null,
                TaskRunStatus.resolve(status),
                startTime,
                endTime
        );
        return RequestResult.success(deployedTaskService.searchTaskRun(deploySearchRequest));
    }

    @GetMapping("/deployed-taskruns")
    @ApiOperation("Search scheduled taskruns")
    public RequestResult<PaginationResult<TaskRun>> searchDeploys(
            @RequestParam(defaultValue = "1") int pageNum,
            @RequestParam(defaultValue = "100") int pageSize,
            @RequestParam(required = false) Optional<Long> ownerId,
            @RequestParam(required = false) List<Long> definitionIds,
            @RequestParam(required = false) String name,
            @RequestParam(required = false) String status,
            @RequestParam(required = false) String taskTemplateName,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime startTime,
            @RequestParam(required = false)
            @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) OffsetDateTime endTime
    ) {
        ScheduledTaskRunSearchRequest deploySearchRequest = new ScheduledTaskRunSearchRequest(
                pageSize,
                pageNum,
                ownerId,
                definitionIds,
                taskTemplateName,
                name,
                TaskRunStatus.resolve(status),
                startTime,
                endTime
        );
        return RequestResult.success(deployedTaskService.searchTaskRun(deploySearchRequest));
    }

    @GetMapping("/deployed-taskruns/{id}")
    @ApiOperation("Get detail of scheduled taskrun")
    public RequestResult<TaskRun> getWorkflowTaskRun(@PathVariable Long id) {
        return RequestResult.success(deployedTaskService.getWorkFlowTaskRun(id));
    }

    @GetMapping("/deployed-taskruns/{id}/log")
    @ApiOperation("Get log of scheduled taskrun")
    public RequestResult<TaskRunLogVO> getWorkflowTaskRunLog(@PathVariable Long id) {
        return RequestResult.success(deployedTaskService.getWorkFlowTaskRunLog(id));
    }

    @GetMapping("/deployed-taskruns/{id}/dag")
    @ApiOperation("Get dag of scheduled taskrun")
    public RequestResult<TaskRunDAG> getWorkflowTaskRunDAG(@PathVariable Long id,
                                                           @RequestParam(defaultValue =  "1") int upstreamLevel,
                                                           @RequestParam(defaultValue =  "1") int downstreamLevel
                                                           ) {
        return RequestResult.success(deployedTaskService.getWorkFlowTaskRunDag(id, upstreamLevel, downstreamLevel));
    }
}
