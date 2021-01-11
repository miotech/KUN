package com.miotech.kun.metadata.databuilder.extract.impl.arango;

import com.google.common.collect.Iterators;
import com.miotech.kun.metadata.core.model.Dataset;
import com.miotech.kun.metadata.databuilder.client.ArangoClient;
import com.miotech.kun.metadata.databuilder.extract.Extractor;
import com.miotech.kun.metadata.databuilder.model.ArangoDataSource;

import java.util.Collection;
import java.util.Iterator;

public class ArangoDatabaseSchemaExtractor implements Extractor {

    private final ArangoDataSource arangoDataSource;
    private final String dbName;
    private final ArangoClient arangoClient;

    public ArangoDatabaseSchemaExtractor(ArangoDataSource arangoDataSource, String dbName) {
        this.arangoDataSource = arangoDataSource;
        this.dbName = dbName;
        this.arangoClient = new ArangoClient(arangoDataSource);
    }

    @Override
    public Iterator<Dataset> extract() {
        try {
            Collection<String> tables = arangoClient.getCollections(this.dbName);
            return Iterators.concat(tables.stream()
                    .filter(collection -> !collection.startsWith("_"))
                    .map(tableName -> new ArangoCollectionSchemaExtractor(this.arangoDataSource, this.dbName, tableName)
                            .extract()
                    ).iterator());
        } finally {
            if (arangoClient != null) {
                arangoClient.close();
            }
        }
    }
}
