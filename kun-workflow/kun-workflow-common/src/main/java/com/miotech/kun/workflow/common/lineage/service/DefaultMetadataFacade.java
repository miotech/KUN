package com.miotech.kun.workflow.common.lineage.service;

import com.miotech.kun.metadata.core.model.DataStore;
import com.miotech.kun.metadata.core.model.Dataset;
import com.miotech.kun.metadata.facade.MetadataServiceFacade;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.Optional;

public class DefaultMetadataFacade implements MetadataFacade {
    private static final ClassPathXmlApplicationContext context;

    private static volatile boolean consumerStarted;

    // service object should be cached
    private static MetadataServiceFacade metadataService;

    static {
        System.setProperty("java.net.preferIPv4Stack", "true");
        context = new ClassPathXmlApplicationContext("metadata-service-consumer.xml");
    }

    @Override
    public Optional<Dataset> fetchDatasetByDatastore(DataStore dataStore) {
        if (!consumerStarted) {
            context.start();
            metadataService = (MetadataServiceFacade) context.getBean(MetadataServiceFacade.getServiceName());
            consumerStarted = true;
        }

        Dataset ds = metadataService.getDatasetByDatastore(dataStore);
        return Optional.ofNullable(ds);
    }
}
