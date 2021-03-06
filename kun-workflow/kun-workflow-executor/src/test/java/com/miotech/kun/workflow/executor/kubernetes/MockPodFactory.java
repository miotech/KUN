package com.miotech.kun.workflow.executor.kubernetes;

import com.miotech.kun.workflow.utils.WorkflowIdGenerator;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodStatus;

import java.util.HashMap;
import java.util.Map;

import static com.miotech.kun.workflow.executor.kubernetes.KubernetesConstants.KUN_TASK_ATTEMPT_ID;
import static com.miotech.kun.workflow.executor.kubernetes.KubernetesConstants.KUN_WORKFLOW;

public class MockPodFactory {

    public static Pod create() {
        long taskRunId = WorkflowIdGenerator.nextTaskRunId();
        long taskAttemptId = WorkflowIdGenerator.nextTaskAttemptId(taskRunId, 1);
        return create(taskAttemptId);
    }

    public static Pod create(long taskAttemptId) {
        return create(taskAttemptId, "Pending");
    }

    public static Pod create(long taskAttemptId, String nameSpace) {
        return create(taskAttemptId, nameSpace, null);
    }

    public static Pod create(long taskAttemptId, String nameSpace, String status) {
        String workerId = KUN_WORKFLOW + taskAttemptId;
        ObjectMeta meta = new ObjectMeta();
        Map<String, String> labels = new HashMap<>();
        labels.put(KUN_TASK_ATTEMPT_ID, String.valueOf(taskAttemptId));
        labels.put(KUN_WORKFLOW, null);
        meta.setLabels(labels);
        meta.setName(workerId);
        meta.setNamespace(nameSpace);
        Pod pod = new Pod();
        pod.setMetadata(meta);
        PodStatus podStatus = new PodStatus();
        podStatus.setPhase(status);
        pod.setStatus(podStatus);
        return pod;
    }
}
