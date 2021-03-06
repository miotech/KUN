package com.miotech.kun.workflow.common.graph;

import com.google.common.collect.ImmutableList;
import com.miotech.kun.commons.utils.ExceptionUtils;
import com.miotech.kun.workflow.core.model.common.Tick;
import com.miotech.kun.workflow.core.model.task.Task;
import com.miotech.kun.workflow.core.model.task.TaskDependency;
import com.miotech.kun.workflow.core.model.task.TaskGraph;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.stream.Collectors;

public class DirectTaskGraph implements TaskGraph {
    private final List<Task> tasks;
    private final AtomicBoolean consumed;

    public DirectTaskGraph(Task... tasks) {
        this(Arrays.asList(tasks));
    }

    public DirectTaskGraph(List<Task> tasks) {
        validateTasks(tasks);
        this.tasks = ImmutableList.copyOf(tasks);
        this.consumed = new AtomicBoolean(false);
    }

    @Override
    public List<Task> tasksScheduledAt(Tick tick) {
        if (consumed.compareAndSet(false, true)) {
            return resolveMutualDependencies(tasks);
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public void updateTasksNextExecutionTick(Tick tick, List<Task> scheduledTasks) {
        return;
    }

    public List<Task> getTasks() {
        return tasks;
    }

    private List<Task> resolveMutualDependencies(List<Task> tasks) {
        Map<Long, Task> lookupTable = tasks.stream().collect(
                Collectors.toMap(Task::getId, Function.identity()));

        List<Task> result = new ArrayList<>(tasks.size());
        for (Task task : tasks) {
            List<TaskDependency> modifiedDependencies = task.getDependencies().stream()
                    .filter(d -> lookupTable.containsKey(d.getUpstreamTaskId()))
                    .collect(Collectors.toList());
            result.add(task.cloneBuilder().withDependencies(modifiedDependencies).build());
        }

        return topoSort(result);
    }

    private List<Task> topoSort(List<Task> taskList) {
        Map<Long, List<Long>> upstreamDependencyMap = coverUpstreamDependencyMap(taskList);
        List<Task> sortTasks = new ArrayList<>();
        Queue<Task> queue = new LinkedList<>();
        int size = taskList.size();
        do {
            if (!queue.isEmpty()) {
                Task sortedTask = queue.poll();
                upstreamDependencyMap.remove(sortedTask.getId());
                sortTasks.add(sortedTask);
            }
            Iterator<Task> iterator = taskList.listIterator();
            while (iterator.hasNext()) {
                Task task = iterator.next();
                if (!taskHasUpstream(task, upstreamDependencyMap)) {
                    queue.offer(task);
                    iterator.remove();
                }
            }
        } while (!queue.isEmpty());
        if (sortTasks.size() != size) {
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

    private Map<Long, List<Long>> coverUpstreamDependencyMap(List<Task> taskList) {
        Map<Long, List<Long>> upstreamDependencyMap = new HashMap<>();
        for (Task task : taskList) {
            List<Long> upstreamDependency = task.getDependencies().stream().
                    map(TaskDependency::getUpstreamTaskId).collect(Collectors.toList());
            upstreamDependencyMap.put(task.getId(), upstreamDependency);
        }
        return upstreamDependencyMap;
    }


    private void validateTasks(List<Task> tasks) {
        for (Task t : tasks) {
            if (t.getId() == null) {
                throw new IllegalArgumentException("Task to build DirectTaskGraph must have id. task=" + t);
            }
        }
    }
}
