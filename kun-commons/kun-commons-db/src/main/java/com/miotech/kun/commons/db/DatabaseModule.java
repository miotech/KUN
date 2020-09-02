package com.miotech.kun.commons.db;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.neo4j.ogm.config.Configuration;
import org.neo4j.ogm.session.SessionFactory;

import javax.sql.DataSource;
import java.util.Properties;

public class DatabaseModule extends AbstractModule {
    public static final String[] NEO4J_DOMAIN_CLASSES = {
            "com.miotech.kun.workflow.common.lineage.node",
            "com.miotech.kun.workflow.common.lineage.relation"
    };

    @Singleton
    @Provides
    public DataSource createDataSource(Properties props) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(props.getProperty("datasource.jdbcUrl"));
        config.setUsername(props.getProperty("datasource.username"));
        config.setPassword(props.getProperty("datasource.password"));
        config.setDriverClassName(props.getProperty("datasource.driverClassName"));
        return new HikariDataSource(config);
    }

    @Singleton
    @Provides
    public SessionFactory provideNeo4jSessionFactory(Properties props) {
        Configuration config = new Configuration.Builder()
                .uri(props.getProperty("neo4j.uri"))
                .connectionPoolSize(50)
                .credentials(props.getProperty("neo4j.username"), props.getProperty("neo4j.password"))
                .build();
        return new SessionFactory(config, NEO4J_DOMAIN_CLASSES);
    }
}
