package com.miotech.kun.dataquality.controller;

import com.google.common.collect.Lists;
import com.miotech.kun.common.model.RequestResult;
import com.miotech.kun.common.model.vo.IdVO;
import com.miotech.kun.common.utils.DateUtils;
import com.miotech.kun.common.utils.IdUtils;
import com.miotech.kun.common.utils.JSONUtils;
import com.miotech.kun.common.utils.WorkflowUtils;
import com.miotech.kun.dataquality.model.bo.*;
import com.miotech.kun.dataquality.model.entity.*;
import com.miotech.kun.dataquality.service.DataQualityService;
import com.miotech.kun.dataquality.service.WorkflowService;
import com.miotech.kun.dataquality.utils.Constants;
import com.miotech.kun.workflow.client.WorkflowClient;
import com.miotech.kun.workflow.client.model.TaskRun;
import com.miotech.kun.workflow.client.model.TaskRunSearchRequest;
import com.miotech.kun.workflow.utils.DateTimeUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: Jie Chen
 * @created: 2020/7/16
 */
@RestController
@RequestMapping("/kun/api/v1")
@Slf4j
public class DataQualityController {

    @Autowired
    DataQualityService dataQualityService;

    @Autowired
    WorkflowService workflowService;

    @Autowired
    WorkflowClient workflowClient;

    @PostMapping("/data-quality/recreate-all-task")
    public void recreateAllTasks() {
        for (Long caseId : dataQualityService.getAllCaseId()) {
            workflowService.deleteTaskByCase(caseId);
            workflowService.createTask(caseId);
        }
    }

    @PostMapping("/data-quality/execute-all-task")
    public void executeAllTasks() {
        //workflowService.executeTask(dataQualityService.getAllCaseId().get(0));
        for (Long caseId : dataQualityService.getAllCaseId()) {
            workflowService.executeTask(caseId);
        }
    }

    @PostMapping("/data-quality/delete-all-task")
    public void deleteAllTasks() {
        for (Long taskId : dataQualityService.getAllTaskId()) {
            workflowService.deleteTask(taskId);
        }
    }

    @GetMapping("/data-quality/search-log")
    public void searchLog(SearchLogRequest searchLogRequest) {
        List<Long> taskIds = dataQualityService.getAllTaskId();
        TaskRunSearchRequest searchRequest = TaskRunSearchRequest.newBuilder()
                .withTaskIds(taskIds)
                .withDateFrom(searchLogRequest.getStartTime() == null ?
                        DateTimeUtils.now().minusDays(1) : DateUtils.millisToOffsetDateTime(searchLogRequest.getStartTime()))
                .withDateTo(searchLogRequest.getEndTime() == null ?
                        DateTimeUtils.now() : DateUtils.millisToOffsetDateTime(searchLogRequest.getEndTime()))
                .withPageNum(1)
                .withPageSize(Integer.MAX_VALUE)
                .build();
        List<DataQualityCaseResult> results = Lists.newArrayList();
        List<TaskRun> taskRuns = workflowClient.searchTaskRun(searchRequest).getRecords();
        for (TaskRun taskRun : taskRuns) {
            List<String> logs = workflowClient.getLatestRunLog(taskRun.getId()).getLogs();

            DataQualityCaseResult result = null;
            if (!CollectionUtils.isEmpty(logs)) {
                Optional<String> resultLog = logs.stream().filter(logLine -> StringUtils.isNotEmpty(logLine) && logLine.contains(Constants.DQ_RESULT_PREFIX)).findFirst();
                if (resultLog.isPresent()) {
                    String json = resultLog.get();
                    json = json.substring(json.indexOf(Constants.DQ_RESULT_PREFIX) + Constants.DQ_RESULT_PREFIX.length());
                    result = JSONUtils.toJavaObject(json, DataQualityCaseResult.class);
                }
            }

            if (result == null) {
                result = new DataQualityCaseResult();
                result.setCaseStatus(Constants.DQ_RESULT_FAILED);
                result.setErrorReason(Lists.newArrayList("No execution log found."));
            }
            result.setTaskId(taskRun.getTask().getId());
            result.setTaskRunId(taskRun.getId());
            log.info(JSONUtils.toJsonString(result));
            results.add(result);
        }

        if (searchLogRequest.isExport()) {
            dataQualityService.logDataQualityCaseResults(results);
        }
    }

    @GetMapping("/data-quality/history")
    public RequestResult<List<DataQualityHistoryRecords>> getHistory(DataQualityHistoryRequest request) {
        return RequestResult.success(dataQualityService.getHistory(request));
    }

    @GetMapping("/data-quality/dimension/get-config")
    public RequestResult<DimensionConfig> getDimensionConfig(@RequestParam("datasourceType") String dsType) {
        return RequestResult.success(dataQualityService.getDimensionConfig(dsType));
    }

    @PostMapping("/data-quality/add")
    @Transactional(rollbackFor = Exception.class)
    public RequestResult<IdVO> addCase(@RequestBody DataQualityRequest dataQualityRequest) {
        IdVO vo = new IdVO();
        vo.setId(dataQualityService.addCase(dataQualityRequest));
        Long taskId = workflowService.executeTask(vo.getId());
        dataQualityService.saveTaskId(vo.getId(), taskId);
        return RequestResult.success(vo);
    }

    @GetMapping("/data-quality/{id}")
    public RequestResult<DataQualityCase> getCase(@PathVariable("id") Long id) {
        return RequestResult.success(dataQualityService.getCase(id));
    }

    @GetMapping("/data-qualities")
    public RequestResult<DataQualityCaseBasics> getCasesByGid(DataQualitiesRequest request) {
        DataQualityCaseBasics caseBasics = dataQualityService.getCasesByGid(request);
        return RequestResult.success(caseBasics);
    }

    @PostMapping("/sql/validate")
    public RequestResult<ValidateSqlResult> validateSql(@RequestBody ValidateSqlRequest request) {
        return RequestResult.success(dataQualityService.validateSql(request));
    }

    @PostMapping("/data-quality/{id}/edit")
    public RequestResult<IdVO> updateCase(@PathVariable("id") Long id,
                                          @RequestBody DataQualityRequest dataQualityRequest) {
        IdVO vo = new IdVO();
        vo.setId(dataQualityService.updateCase(id, dataQualityRequest));
        Long taskId = workflowService.executeTask(vo.getId());
        dataQualityService.saveTaskId(vo.getId(), taskId);
        return RequestResult.success(vo);
    }

    @DeleteMapping("/data-quality/{id}/delete")
    public RequestResult<IdVO> deleteCase(@PathVariable("id") Long id) {
        workflowService.deleteTaskByCase(id);
        DeleteCaseResponse response = dataQualityService.deleteCase(id);
        IdVO vo = new IdVO();
        vo.setId(response.getId());

        return RequestResult.success(vo);
    }
}
