package com.miotech.kun.metadata.databuilder.schedule;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.miotech.kun.commons.db.DatabaseOperator;
import com.miotech.kun.commons.utils.ExceptionUtils;
import com.miotech.kun.metadata.databuilder.client.GlueClient;
import com.miotech.kun.metadata.databuilder.constant.DatabaseType;
import com.miotech.kun.metadata.databuilder.extract.impl.arango.ArangoCollectionExtractor;
import com.miotech.kun.metadata.databuilder.extract.impl.arango.ArangoExtractor;
import com.miotech.kun.metadata.databuilder.extract.impl.elasticsearch.ElasticSearchIndexExtractor;
import com.miotech.kun.metadata.databuilder.extract.impl.elasticsearch.ElasticsearchExtractor;
import com.miotech.kun.metadata.databuilder.extract.impl.glue.GlueExtractor;
import com.miotech.kun.metadata.databuilder.extract.impl.glue.GlueTableExtractor;
import com.miotech.kun.metadata.databuilder.extract.impl.mongo.MongoCollectionExtractor;
import com.miotech.kun.metadata.databuilder.extract.impl.mongo.MongoExtractor;
import com.miotech.kun.metadata.databuilder.extract.impl.postgres.PostgresExtractor;
import com.miotech.kun.metadata.databuilder.extract.impl.postgres.PostgresTableExtractor;
import com.miotech.kun.metadata.databuilder.extract.tool.ConnectUrlUtil;
import com.miotech.kun.metadata.databuilder.load.Loader;
import com.miotech.kun.metadata.databuilder.load.impl.PostgresLoader;
import com.miotech.kun.metadata.databuilder.model.*;
import com.miotech.kun.metadata.databuilder.service.gid.DataStoreJsonUtil;
import com.miotech.kun.workflow.core.model.lineage.*;
import com.miotech.kun.workflow.utils.JSONUtils;
import io.prestosql.jdbc.$internal.guava.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

@Singleton
public class DataBuilder {

    private static final Logger logger = LoggerFactory.getLogger(DataBuilder.class);

    private final AtomicBoolean scheduled;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(
            1,
            new ThreadFactoryBuilder().setNameFormat("data-builder-scheduler").build()
    );

    private final ExecutorService threadPool = Executors.newFixedThreadPool(5);

    private final DatabaseOperator operator;

    private final Loader loader;

    private Map<Long, List<String>> newestStates = Maps.newConcurrentMap();

    @Inject
    public DataBuilder(DatabaseOperator operator) {
        this.scheduled = new AtomicBoolean(false);
        this.operator = operator;
        this.loader = new PostgresLoader(operator);
    }

    public Map<Long, List<String>> getNewestStates() {
        return newestStates;
    }

    public void buildAll() {
        String sql = "SELECT kmd.id, kmdt.name, kmd.connection_info FROM kun_mt_datasource kmd JOIN kun_mt_datasource_type kmdt ON kmd.type_id = kmdt.id";
        List<DataSource> dataSources = operator.fetchAll(sql, rs -> generateDataSource(rs.getLong(1), rs.getString(2), rs.getString(3)));
        CountDownLatch countDownLatch = new CountDownLatch(dataSources.size());
        for (DataSource dataSource : dataSources) {
            threadPool.submit(() -> {
                try {
                    build(dataSource);
                } catch (Exception e) {
                    logger.error("DataBuilder buildAll build fail, dataSource: {}", JSONUtils.toJsonString(dataSource), e);
                } finally {
                    countDownLatch.countDown();
                }

            });
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            logger.error("DataBuilder buildAll await error");
            throw ExceptionUtils.wrapIfChecked(e);
        }
    }

    public void buildDatasource(long datasourceId) {
        Preconditions.checkArgument(datasourceId > 0L, "datasourceId must be a positive long, datasourceId: %s", datasourceId);

        String sql = "SELECT kmd.id, kmdt.name, kmd.connection_info FROM kun_mt_datasource kmd JOIN kun_mt_datasource_type kmdt ON kmd.type_id = kmdt.id WHERE kmd.id = ?";
        DataSource dataSource = operator.fetchOne(sql, rs -> generateDataSource(rs.getLong(1), rs.getString(2), rs.getString(3)), datasourceId);
        build(dataSource);
    }

    public void buildDataset(Long gid) {
        Long datasourceId = operator.fetchOne("SELECT datasource_id FROM kun_mt_dataset WHERE gid = ?", rs -> rs.getLong(1), gid);
        Preconditions.checkNotNull(datasourceId, "Invalid param `gid`: " + gid + " No corresponding datasource found");

        String sql = "SELECT kmds.id, kmdst.name, kmds.connection_info, kmd.data_store FROM kun_mt_dataset kmd JOIN kun_mt_datasource kmds ON kmd.datasource_id = kmds.id JOIN kun_mt_datasource_type kmdst ON kmds.type_id = kmdst.id WHERE kmd.gid = ?";
        DatasetConnDto datasetConnDto = operator.fetchOne(sql, this::buildDatasetConnDto, gid);
        build(datasetConnDto);
    }

    private DataSource generateDataSource(long id, String datasourceType, String connStr) throws SQLException {
        DataSource.Type type = DataSource.Type.valueOf(datasourceType.toUpperCase());

        if (type.equals(DataSource.Type.AWS)) {
            AWSDataSource awsConnection = JSONUtils.jsonToObject(connStr, AWSDataSource.class);
            return AWSDataSource.clone(awsConnection).withId(id).build();
        }

        JDBCConnection jdbcConnection = JSONUtils.jsonToObject(connStr, JDBCConnection.class);
        switch (type) {
            case POSTGRESQL:
                PostgresDataSource.Builder postgresDataSourceBuilder = PostgresDataSource.newBuilder();
                postgresDataSourceBuilder.withId(id)
                        .withUrl(ConnectUrlUtil.convertToConnectUrl(jdbcConnection.getHost(), jdbcConnection.getPort(),
                                jdbcConnection.getUsername(), jdbcConnection.getPassword(), DatabaseType.POSTGRES))
                        .withUsername(jdbcConnection.getUsername())
                        .withPassword(jdbcConnection.getPassword());
                return postgresDataSourceBuilder.build();
            case MONGODB:
                MongoDataSource.Builder mongoDataSourceBuilder = MongoDataSource.newBuilder();
                mongoDataSourceBuilder.withId(id)
                        .withUrl(ConnectUrlUtil.convertToConnectUrl(jdbcConnection.getHost(), jdbcConnection.getPort(),
                                jdbcConnection.getUsername(), jdbcConnection.getPassword(), DatabaseType.MONGO))
                        .withUsername(jdbcConnection.getUsername())
                        .withPassword(jdbcConnection.getPassword());
                return mongoDataSourceBuilder.build();
            case ELASTICSEARCH:
                ElasticSearchDataSource elasticSearchDataSource = ElasticSearchDataSource.newBuilder()
                        .withId(id)
                        .withUrl(ConnectUrlUtil.convertToConnectUrl(jdbcConnection.getHost(), jdbcConnection.getPort(),
                                jdbcConnection.getUsername(), jdbcConnection.getPassword(), DatabaseType.ELASTICSEARCH))
                        .withUsername(jdbcConnection.getUsername())
                        .withPassword(jdbcConnection.getPassword())
                        .build();
                return elasticSearchDataSource;
            case ARANGO:
                ArangoDataSource arangoDataSource = ArangoDataSource.newBuilder()
                        .withId(id)
                        .withUrl(ConnectUrlUtil.convertToConnectUrl(jdbcConnection.getHost(), jdbcConnection.getPort(),
                                jdbcConnection.getUsername(), jdbcConnection.getPassword(), DatabaseType.ARANGO))
                        .withUsername(jdbcConnection.getUsername())
                        .withPassword(jdbcConnection.getPassword())
                        .build();
                return arangoDataSource;
            default:
                logger.error("Invalid datasource type: {}", type);
                throw new UnsupportedOperationException("Invalid datasource type: " + type);
        }
    }

    private DatasetConnDto buildDatasetConnDto(ResultSet resultSet) throws SQLException {
        DatasetConnDto.Builder datasetConnDtoBuilder = DatasetConnDto.newBuilder();

        DataSource dataSource = generateDataSource(resultSet.getLong(1), resultSet.getString(2), resultSet.getString(3));
        datasetConnDtoBuilder.withDataSource(dataSource);

        try {
            String dataStoreStr = resultSet.getString(4);
            datasetConnDtoBuilder.withDataStore(DataStoreJsonUtil.toDataStore(dataStoreStr));
        } catch (JsonProcessingException jsonProcessingException) {
            logger.error("DataStoreJsonUtil.toDataStore error: ", jsonProcessingException);
            throw ExceptionUtils.wrapIfChecked(jsonProcessingException);
        }
        return datasetConnDtoBuilder.build();
    }

    private void build(DatasetConnDto datasetConnDto) {
        Preconditions.checkNotNull(datasetConnDto, "datasetConnDto should not be null.");
        recordStates(datasetConnDto.getDataSource().getId(), datasetConnDto.getDataStore());
        try {
            DataSource dataSource = datasetConnDto.getDataSource();
            DataStore dataStore = datasetConnDto.getDataStore();
            Dataset dataset = null;
            if (dataSource instanceof AWSDataSource) {
                AWSDataSource awsDataSource = (AWSDataSource) dataSource;
                HiveTableStore hiveTableStore = (HiveTableStore) dataStore;
                dataset = new GlueTableExtractor(awsDataSource, GlueClient.searchTable(awsDataSource, hiveTableStore.getDatabase(), hiveTableStore.getTable())).extract().next();
            } else if (dataSource instanceof PostgresDataSource) {
                PostgresDataSource pgDataSource = (PostgresDataSource) dataSource;
                PostgresDataStore pgDataStore = (PostgresDataStore) dataStore;
                dataset = new PostgresTableExtractor(pgDataSource,
                        pgDataStore.getDatabase(),
                        pgDataStore.getSchema(),
                        pgDataStore.getTableName()
                ).extract().next();
            } else if (dataSource instanceof MongoDataSource) {
                dataset = new MongoCollectionExtractor(((MongoDataSource) dataSource),
                        ((MongoDataStore) dataStore).getDatabase(),
                        ((MongoDataStore) dataStore).getCollection()
                ).extract().next();
            } else if (dataSource instanceof ElasticSearchDataSource) {
                ElasticSearchDataSource elasticSearchDataSource = (ElasticSearchDataSource) dataSource;
                dataset = new ElasticSearchIndexExtractor(elasticSearchDataSource,
                        ((ElasticSearchIndexStore) dataStore).getIndex()
                ).extract().next();
            } else if (dataSource instanceof ArangoDataSource) {
                dataset = new ArangoCollectionExtractor(((ArangoDataSource) dataSource),
                        ((ArangoCollectionStore) dataStore).getDatabase(),
                        ((ArangoCollectionStore) dataStore).getCollection()
                ).extract().next();
            }

            try {
                loader.load(dataset);
                mark(newestStates.getOrDefault(dataSource.getId(), Lists.newCopyOnWriteArrayList()), dataset);
            } catch (Exception e) {
                logger.error("load error: ", e);
            }
        } catch (Exception e) {
            logger.error("build dataset error: ", e);
        }
    }


    private void build(DataSource dataSource) {
        Preconditions.checkNotNull(dataSource, "dataSource should not be null.");
        recordStates(dataSource.getId());
        try {
            Iterator<Dataset> datasetIterator = null;
            if (dataSource instanceof AWSDataSource) {
                datasetIterator = new GlueExtractor((AWSDataSource) dataSource).extract();
            } else if (dataSource instanceof PostgresDataSource) {
                datasetIterator = new PostgresExtractor((PostgresDataSource) dataSource).extract();
            } else if (dataSource instanceof MongoDataSource) {
                datasetIterator = new MongoExtractor((MongoDataSource) dataSource).extract();
            } else if (dataSource instanceof ElasticSearchDataSource) {
                ElasticSearchDataSource elasticSearchDataSource = (ElasticSearchDataSource) dataSource;
                datasetIterator = new ElasticsearchExtractor(elasticSearchDataSource).extract();
            } else if (dataSource instanceof ArangoDataSource) {
                datasetIterator = new ArangoExtractor((ArangoDataSource) dataSource).extract();
            }

            if (datasetIterator != null) {
                while (datasetIterator.hasNext()) {
                    try {
                        Dataset dataset = datasetIterator.next();
                        loader.load(dataset);

                        mark(newestStates.getOrDefault(dataSource.getId(), Lists.newCopyOnWriteArrayList()), dataset);
                    } catch (Exception e) {
                        logger.error("etl next error: ", e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("start etl error: ", e);
        }
    }

    public void scheduleAtRate(long initialDelay, long period, TimeUnit unit) {
        if (scheduled.compareAndSet(false, true)) {
            logger.info("Start scheduling buildAll task. period={}, unit={}", period, unit);
            scheduler.scheduleAtFixedRate(this::buildAll, initialDelay, period, unit);
        } else {
            throw new IllegalStateException("BuildAll task is already scheduled.");
        }
    }

    private void recordStates(long datasourceId) {
        List<String> dataStores = operator.fetchAll("SELECT data_store FROM kun_mt_dataset WHERE datasource_id = ?",
                rs -> rs.getString(1), datasourceId);
        for (String dataStore : dataStores) {
            if (newestStates.containsKey(datasourceId)) {
                newestStates.get(datasourceId).add(dataStore);
            } else {
                List<String> dataStoreSnapshots = Lists.newCopyOnWriteArrayList();
                dataStoreSnapshots.add(dataStore);
                newestStates.put(datasourceId, dataStoreSnapshots);
            }

        }
    }

    private void recordStates(long datasourceId, DataStore dataStore) {
        try {
            List<String> dataStores = Lists.newCopyOnWriteArrayList();
            dataStores.add(DataStoreJsonUtil.toJson(dataStore));
            newestStates.put(datasourceId, dataStores);
        } catch (JsonProcessingException jsonProcessingException) {
            throw ExceptionUtils.wrapIfChecked(jsonProcessingException);
        }
    }

    private void mark(List<String> dataStores, Dataset build) throws JsonProcessingException {
        Iterator<String> dataStoreIterator = dataStores.iterator();
        while (dataStoreIterator.hasNext()) {
            String dataStoreInSnapshot = dataStoreIterator.next();
            if (dataStoreInSnapshot.equals(DataStoreJsonUtil.toJson(build.getDataStore()))) {
                dataStoreIterator.remove();
            }
        }
    }

    public void sweep() {
        for (Map.Entry<Long, List<String>> entry : newestStates.entrySet()) {
            for (String dataStore : entry.getValue()) {
                operator.update("DELETE FROM kun_mt_dataset WHERE data_store = CAST(? AS JSONB)", dataStore);
            }
        }
    }
}
