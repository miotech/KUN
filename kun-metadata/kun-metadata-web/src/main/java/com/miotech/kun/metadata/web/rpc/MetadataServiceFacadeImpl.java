package com.miotech.kun.metadata.web.rpc;

import com.miotech.kun.commons.rpc.KunRemoteServiceProvider;
import com.miotech.kun.metadata.core.model.DataStore;
import com.miotech.kun.metadata.core.model.Dataset;
import com.miotech.kun.metadata.facade.MetadataServiceFacade;

public class MetadataServiceFacadeImpl implements MetadataServiceFacade, KunRemoteServiceProvider {
    public static final String META_DATA_SERVICE_NAME = "KUN_METADATA_SERVICE_PROVIDER";

    @Override
    public void bootstrap() {
    }

    @Override
    public String getServiceName() {
        return META_DATA_SERVICE_NAME;
    }

    @Override
    public Dataset getDatasetByDatastore(DataStore datastore) {
        return null;
    }
}
