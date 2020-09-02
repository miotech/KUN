package com.miotech.kun.workflow.core.execution;

import com.miotech.kun.workflow.core.model.lineage.DataStore;

import java.util.List;

/**
 * Operator configuration resolver. Resolves upstream & downstream data lineages.
 */
public interface Resolver {
    List<DataStore> resolveUpstreamDataStore(Config config);

    List<DataStore> resolveDownstreamDataStore(Config config);
}
