package com.miotech.kun.workflow.operator;

import com.miotech.kun.workflow.core.execution.OperatorContext;
import com.miotech.kun.workflow.core.execution.logging.Logger;
import com.miotech.kun.workflow.core.model.lineage.DataStore;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OperatorContextImpl implements OperatorContext {

    private Map<String, String> params = new HashMap();
    private Map<String, String> vars = new HashMap();

    public OperatorContextImpl(){
//        params.put("files", "s3://com.miotech.data.prd/Project/miotech/knowledge-graph-etl/release/com/miotech/knowledge-graph-etl/1.0/knowledge-graph-etl-1.0.jar");
//        params.put("application", "com.miotech.etl.knowledge_graph.app.Application");
//        params.put("args", "-e dev -m TASK_CENTER -s $STEP -i 51257436958359552");
//        params.put("name", "test_operator_3");
//        params.put("livyHost", "http://<livy_ip>:8998");
//        params.put("dispatcher", "s3");
//        vars.put("STEP", "CHOICE_DM_FULLY_UPDATE_STEP");

        params.put("files", "local:/Users/aijiaguo/git/spline-spark-agent/bundle-2.4/target/spark-2.4-spline-agent-bundle_2.11-0.6.0-SNAPSHOT.jar");
        params.put("application", "za.co.absa.spline.example.EsJob");
        params.put("args", "--packages org.elasticsearch:elasticsearch-hadoop:7.6.0");
        params.put("name", "test_operator");
        params.put("queue", "narrative");
        params.put("livyHost", "http://localhost:8998");
        params.put("dispatcher", "s3");


    }

    public String getParameter(String name){
        return params.getOrDefault(name, "");
    }

    public String getVariable(String name){
        return vars.getOrDefault(name, "");
    }

    public Logger getLogger() {
        return (Logger) LoggerFactory.getLogger(OperatorContextImpl.class);
    }

    public void report(List<DataStore> inlets, List<DataStore> outlets){}


}
