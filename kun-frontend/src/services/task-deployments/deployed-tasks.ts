import { get } from '@/utils/requestUtils';
import { API_DATA_PLATFORM_PREFIX } from '@/constants/api-prefixes';

import { PaginationReqBody, PaginationRespBody, ServiceRespPromise } from '@/definitions/common-types';
import { TaskRun, TaskRunDAG, TaskRunLog } from '@/definitions/TaskRun.type';
import { RunStatusEnum } from '@/definitions/StatEnums.type';
import { DeployedTask, DeployedTaskDAG, DeployedTaskDetail } from '@/definitions/DeployedTask.type';

/**
 * GET /deploy-taskruns
 * Search scheduled taskruns
 */

export interface FetchScheduledTaskRunsReqParams extends Partial<PaginationReqBody> {
  name?: string;
  definitionsIds?: string[];
  ownerId?: (number | string)[];
  status?: RunStatusEnum;
  taskTemplateName?: string;
}

export async function fetchScheduledTaskRuns(reqParams: FetchScheduledTaskRunsReqParams = {}): ServiceRespPromise<PaginationRespBody<TaskRun>> {
  return get('/deployed-taskruns', {
    query: { ...reqParams },
    prefix: API_DATA_PLATFORM_PREFIX,
    mockCode: 'deployed-taskruns.search',
  });
}

/**
 * GET /deployed-taskruns/{id}
 * Get detail of scheduled taskrun
 */

export async function fetchDetailedScheduledTaskRun(taskRunId: string): ServiceRespPromise<TaskRun> {
  return get('/deployed-taskruns/:id', {
    pathParams: { id: taskRunId },
    prefix: API_DATA_PLATFORM_PREFIX,
  });
}

/**
 * GET /deployed-taskruns/{id}/log
 * Get log of scheduled taskrun
 */

export async function fetchScheduledTaskRunLog(taskRunId: string): ServiceRespPromise<TaskRunLog> {
  return get('/deployed-taskruns/:id/log', {
    pathParams: { id: taskRunId },
    prefix: API_DATA_PLATFORM_PREFIX,
  });
}

/**
 * GET /deployed-tasks
 * Search deployed tasks
 */

export interface FetchDeployedTasksReqParams extends Partial<PaginationReqBody> {
  name?: string;
  definitionsIds?: string[];
  ownerId?: (number | string)[];
  status?: RunStatusEnum;
  taskTemplateName?: string;
}

export async function fetchDeployedTasks(reqParams: FetchDeployedTasksReqParams): ServiceRespPromise<PaginationRespBody<DeployedTask>> {
  return get('/deployed-tasks', {
    query: { ...reqParams },
    prefix: API_DATA_PLATFORM_PREFIX,
    mockCode: 'deployed-tasks.search',
  });
}

/**
 * GET /deployed-tasks/{id}
 * Get deployed task
 */

export async function fetchDeployedTaskDetail(deployedTaskId: string): ServiceRespPromise<DeployedTaskDetail> {
  return get('/deployed-tasks/:id', {
    pathParams: { id: deployedTaskId },
    prefix: API_DATA_PLATFORM_PREFIX,
    mockCode: 'deployed-tasks.get-detail',
  });
}

/**
 * GET /deployed-tasks/{id}/dag
 * Get dag of deployed task
 */

export interface FetchDeployedTaskDAGOptionParams {
  upstreamLevel?: number;
  downstreamLevel?: number;
}

export async function fetchDeployedTaskDAG(
  deployedTaskId: string,
  optionParams: FetchDeployedTaskDAGOptionParams = {},
): ServiceRespPromise<DeployedTaskDAG> {
  return get('/deployed-tasks/:id/dag', {
    query: { ...optionParams },
    pathParams: {
      id: deployedTaskId,
    },
    prefix: API_DATA_PLATFORM_PREFIX,
    mockCode: 'deployed-tasks.get-dag',
  });
}

/**
 * GET /deployed-tasks/{id}/taskruns
 * Search task runs of deployed task
 */

export interface FetchTaskRunsOfDeployedTaskReqParams extends Partial<PaginationReqBody> {
  id: string;
  startTime?: string;
  endTime?: string;
  status?: RunStatusEnum;
}

export async function fetchTaskRunsOfDeployedTask(reqParams: FetchTaskRunsOfDeployedTaskReqParams): ServiceRespPromise<PaginationRespBody<TaskRun>> {
  const { id, ...restParams } = reqParams;
  return get('/deployed-tasks/:id/taskruns', {
    pathParams: { id },
    query: { ...restParams },
    prefix: API_DATA_PLATFORM_PREFIX,
    mockCode: 'deployed-tasks.get-task-runs',
  });
}

/**
 * GET /deployed-taskruns/{id}/dag
 * Get dag of deployed task
 */

export interface FetchTaskRunDAGOptionParams {
  upstreamLevel?: number;
  downstreamLevel?: number;
}

export async function fetchTaskRunDAG(
  taskRunId: string,
  optionParams: FetchTaskRunDAGOptionParams = {},
): ServiceRespPromise<TaskRunDAG> {
  return get('/deployed-taskruns/:id/dag', {
    query: { ...optionParams },
    pathParams: {
      id: taskRunId,
    },
    prefix: API_DATA_PLATFORM_PREFIX,
    mockCode: 'deployed-tasks.get-taskrun-dag',
  });
}