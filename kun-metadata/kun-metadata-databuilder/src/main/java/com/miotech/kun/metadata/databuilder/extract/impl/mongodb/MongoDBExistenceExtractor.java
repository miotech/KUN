package com.miotech.kun.metadata.databuilder.extract.impl.mongodb;

import com.miotech.kun.metadata.core.model.dataset.Dataset;
import com.google.common.collect.Lists;
import com.miotech.kun.metadata.databuilder.constant.DatasetExistenceJudgeMode;
import com.miotech.kun.metadata.databuilder.extract.schema.DatasetExistenceExtractor;
import com.miotech.kun.metadata.databuilder.model.DataSource;
import com.miotech.kun.metadata.databuilder.model.MongoDataSource;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;

public class MongoDBExistenceExtractor implements DatasetExistenceExtractor {

    @Override
    public boolean judgeExistence(Dataset dataset, DataSource dataSource, DatasetExistenceJudgeMode judgeMode) {
        MongoDataSource mongoDataSource = (MongoDataSource) dataSource;
        try (MongoClient client = new MongoClient(new MongoClientURI(mongoDataSource.getUrl()))) {
            return client.getDatabase(dataset.getDatabaseName()).listCollectionNames().into(Lists.newArrayList()).contains(dataset.getName());
        }
    }

}
