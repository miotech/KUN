package com.miotech.kun.workflow.core.execution;

import com.miotech.kun.workflow.core.execution.logging.Logger;

public interface OperatorContext {
    /**
     * 获取任务的Logger
     */
    public Logger getLogger();

    /**
     * 获取任务的参数
     */
    public String getParameter(String name);

    /**
     * 获取资源，用于支持用户上传jar包类型的任务。
     */
//    public Resource getResource(String path);

    /**
     * 获取任务运行时的变量
     */
    public String getVariable(String name);

    /**
     * 报告任务的输入/输出信息
     */
//    public void report(List<Entity> inlets, List<Entity> outlets);

}
