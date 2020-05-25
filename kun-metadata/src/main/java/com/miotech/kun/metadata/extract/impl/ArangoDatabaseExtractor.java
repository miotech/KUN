package com.miotech.kun.metadata.extract.impl;

import com.google.common.collect.Iterators;
import com.miotech.kun.metadata.extract.Extractor;
import com.miotech.kun.metadata.model.Dataset;
import com.miotech.kun.workflow.core.model.entity.CommonCluster;

import java.util.ArrayList;
import java.util.Iterator;

public class ArangoDatabaseExtractor implements Extractor {

    private CommonCluster cluster;
    private String database;
    private MioArangoClient client;

    public ArangoDatabaseExtractor(CommonCluster cluster, String database){
        this.cluster = cluster;
        this.database = database;
        this.client = new MioArangoClient(cluster);
    }

    @Override
    public Iterator<Dataset> extract() {
        ArrayList<String> tables = (ArrayList<String>) client.getCollections(this.database);
        return Iterators.concat(tables.stream().map((tableName) -> new ArangoCollectionExtractor(this.cluster, this.database, tableName).extract()).iterator());
    }
}
