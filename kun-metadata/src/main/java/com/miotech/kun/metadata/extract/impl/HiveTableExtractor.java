package com.miotech.kun.metadata.extract.impl;

import com.miotech.kun.metadata.client.JDBCClient;
import com.miotech.kun.metadata.constant.DatabaseType;
import com.miotech.kun.metadata.extract.factory.ExtractorTemplate;
import com.miotech.kun.metadata.model.DatasetField;
import com.miotech.kun.metadata.model.DatasetFieldStat;
import com.miotech.kun.metadata.model.DatasetStat;
import com.miotech.kun.workflow.core.model.entity.DataStore;
import com.miotech.kun.workflow.core.model.entity.HiveCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class HiveTableExtractor extends ExtractorTemplate {
    private static Logger logger = LoggerFactory.getLogger(HiveTableExtractor.class);

    private HiveCluster cluster;
    private String database;
    private String table;

    public HiveTableExtractor(HiveCluster cluster, String database, String table) {
        this.cluster = cluster;
        this.database = database;
        this.table = table;
    }

    @Override
    public List<DatasetField> getSchema() {
        // Get schema information of table
        List<DatasetField> fields = new ArrayList<>();
        Connection connection = null;
        PreparedStatement statement = null;
        ResultSet resultSet = null;
        try {
            connection = JDBCClient.getConnection(DatabaseType.MYSQL, cluster.getMetaStoreUrl(),
                    cluster.getMetaStoreUsername(), cluster.getMetaStorePassword());
            String sql = "SELECT source.* FROM  " +
                    "    (SELECT t.TBL_ID, d.NAME as `schema`, t.TBL_NAME name, t.TBL_TYPE, tp.PARAM_VALUE as description,  " +
                    "           p.PKEY_NAME as col_name, p.INTEGER_IDX as col_sort_order,  " +
                    "           p.PKEY_TYPE as col_type, p.PKEY_COMMENT as col_description, 1 as is_partition_col,  " +
                    "           IF(t.TBL_TYPE = 'VIRTUAL_VIEW', 1, 0) is_view " +
                    "    FROM TBLS t" +
                    "    JOIN DBS d ON t.DB_ID = d.DB_ID" +
                    "    JOIN PARTITION_KEYS p ON t.TBL_ID = p.TBL_ID " +
                    "    LEFT JOIN TABLE_PARAMS tp ON (t.TBL_ID = tp.TBL_ID AND tp.PARAM_KEY='comment') " +
                    "    WHERE t.TBL_NAME = ? " +
                    "    UNION " +
                    "    SELECT t.TBL_ID, d.NAME as `schema`, t.TBL_NAME name, t.TBL_TYPE, tp.PARAM_VALUE as description, " +
                    "           c.COLUMN_NAME as col_name, c.INTEGER_IDX as col_sort_order, " +
                    "           c.TYPE_NAME as col_type, c.COMMENT as col_description, 0 as is_partition_col, " +
                    "           IF(t.TBL_TYPE = 'VIRTUAL_VIEW', 1, 0) is_view " +
                    "    FROM TBLS t " +
                    "    JOIN DBS d ON t.DB_ID = d.DB_ID " +
                    "    JOIN SDS s ON t.SD_ID = s.SD_ID " +
                    "    JOIN COLUMNS_V2 c ON s.CD_ID = c.CD_ID " +
                    "    LEFT JOIN TABLE_PARAMS tp ON (t.TBL_ID = tp.TBL_ID AND tp.PARAM_KEY='comment') " +
                    "    WHERE t.TBL_NAME = ? " +
                    "    ) source " +
                    "    ORDER by tbl_id, is_partition_col desc;";
            statement = connection.prepareStatement(sql);

            statement.setString(1, table);
            statement.setString(2, table);
            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                String name = resultSet.getString(6);
                String type = resultSet.getString(8);
                String description = resultSet.getString(7);

                DatasetField field = new DatasetField(name, type, description, null);
                fields.add(field);
            }
        } catch (ClassNotFoundException classNotFoundException) {
            logger.error("driver class not found, DatabaseType: {}", DatabaseType.MYSQL.getName(), classNotFoundException);
            throw new RuntimeException(classNotFoundException);
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        } finally {
            JDBCClient.close(connection, statement, resultSet);
        }

        return fields;
    }

    @Override
    public DatasetFieldStat getFieldStats(DatasetField datasetField) {
        DatasetFieldStat result = new DatasetFieldStat();
        Connection connection;
        PreparedStatement statement = null;
        ResultSet resultSet = null;

        try {
            connection = JDBCClient.getConnection(DatabaseType.HIVE, cluster.getDataStoreUrl() + "/" + database, cluster.getDataStoreUsername(), cluster.getDataStorePassword());
            String useDatabase = "use " + database;
            PreparedStatement useDatabaseStatement = connection.prepareStatement(useDatabase);
            useDatabaseStatement.execute();

            String sql = "SELECT COUNT(*) FROM (SELECT ? FROM " + table + " GROUP BY ?) t1";
            statement = connection.prepareStatement(sql);
            statement.setString(1, datasetField.getName());
            statement.setString(2, datasetField.getName());
            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Long distinctCount = resultSet.getLong(0);
                result.setDistinctCount(distinctCount);
            }

            sql = "SELECT COUNT(*) FROM " + table + " WHERE ? IS NOT NULL)";
            statement = connection.prepareStatement(sql);
            statement.setString(1, datasetField.getName());
            resultSet = statement.executeQuery();

            while (resultSet.next()) {
                Long nonnullCount = resultSet.getLong(1);
                result.setNonnullCount(nonnullCount);
            }

        } catch (ClassNotFoundException classNotFoundException) {
            logger.error("driver class not found, DatabaseType: {}", DatabaseType.MYSQL.getName(), classNotFoundException);
            throw new RuntimeException(classNotFoundException);
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        } finally {
            JDBCClient.close(statement, resultSet);
        }

        return result;
    }

    @Override
    public DatasetStat getTableStats() {
        DatasetStat datasetStat = new DatasetStat();

        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        try {
            connection = JDBCClient.getConnection(DatabaseType.HIVE, cluster.getDataStoreUrl() + "/" + database, cluster.getDataStoreUsername(), cluster.getDataStorePassword());
            String sql = "SELECT COUNT(*) FROM " + table;
            statement = connection.createStatement();
            resultSet = statement.executeQuery(sql);

            while (resultSet.next()) {
                Long rowCount = resultSet.getLong(1);
                datasetStat.setRowCount(rowCount);
                datasetStat.setStatDate(new Date());
            }
        } catch (ClassNotFoundException classNotFoundException) {
            logger.error("driver class not found, DatabaseType: {}", DatabaseType.HIVE.getName(), classNotFoundException);
            throw new RuntimeException(classNotFoundException);
        } catch (SQLException sqlException) {
            throw new RuntimeException(sqlException);
        } finally {
            JDBCClient.close(connection, statement, resultSet);
        }

        return datasetStat;
    }

    @Override
    protected DataStore getDataStore() {
        return null;
    }

}
