package com.miotech.kun.commons.db;

import com.miotech.kun.commons.utils.Props;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.configuration.FluentConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.concurrent.atomic.AtomicBoolean;

public class DatabaseSetup {
    private final Logger logger = LoggerFactory.getLogger(DatabaseSetup.class);

    private final DataSource dataSource;
    private final String[] locations;
    private final AtomicBoolean initialized;
    private final Props props;

    public DatabaseSetup(DataSource dataSource, Props props) {
        this(dataSource, props, new String[]{});
    }

    public DatabaseSetup(DataSource dataSource, Props props, String... locations) {
        this.dataSource = dataSource;
        this.locations = locations;
        this.props = props;
        this.initialized = new AtomicBoolean(false);
    }

    public void start() {
        if (initialized.compareAndSet(false, true)) {

            FluentConfiguration configuration = Flyway.configure()
                .configuration(props.toProperties());

            if (dataSource != null) configuration.dataSource(dataSource);
            if (locations != null && locations.length > 0) {
                configuration.locations(locations);
            }

            Flyway flyway = configuration.load();
            flyway.migrate();
        }
    }
}
