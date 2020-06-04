package com.miotech.kun.metadata.extract.impl;

import com.miotech.kun.metadata.extract.impl.arango.ArangoCollectionExtractor;
import com.miotech.kun.metadata.model.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ArangoCollectionExtractorTest {

    ArangoCollectionExtractor extractor;

    @Before
    public void setUp() throws Exception {
        ArangoDataSource cluster = ArangoDataSource.newBuilder()
                .withDataStoreUrl("10.0.2.162:8529")
                .withDataStoreUsername("root")
                .withDataStorePassword("d@ta")
                .build();

        this.extractor = new ArangoCollectionExtractor(cluster, "miograph_unmerged_two", "mio_people_family_relations");
//        ArangoCluster cluster = ArangoCluster.newBuilder()
//                .withHostname("localhost")
//                .withPort(8529)
//                .withUsername("")
//                .withPassword("")
//                .build();
//
//        this.extractor = new ArangoCollectionExtractor(cluster, "_system", "test");
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void getSchema() {
        List<DatasetField> fields = extractor.getSchema();
        assert !fields.isEmpty();
    }

    @Test
    public void getFieldStats() {
        DatasetField datasetField = new DatasetField("relationDetail", new DatasetFieldType(DatasetFieldType.Type.CHARACTER, "TEXT"), "");
        DatasetFieldStat stat = extractor.getFieldStats(datasetField);
        assert stat.getNonnullCount() > 0;
    }

    @Test
    public void getTableStats() {
        DatasetStat stat = extractor.getTableStats();
        assert stat.getRowCount() > 0;
    }

    @Test
    public void getDataStore() {
    }
}