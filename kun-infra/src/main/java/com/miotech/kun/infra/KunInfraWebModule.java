package com.miotech.kun.infra;

import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.miotech.kun.commons.utils.Props;
import com.miotech.kun.commons.web.modle.BasePackages;
import com.miotech.kun.commons.web.module.KunWebServerModule;
import com.miotech.kun.infra.service.InitService;

import java.util.ArrayList;
import java.util.List;

public class KunInfraWebModule extends KunWebServerModule {
    public KunInfraWebModule(Props props) {
        super(props);
    }

    @Provides
    @Singleton
    public BasePackages getPackageScan() {
        List<String> scanList = new ArrayList<>();
        scanList.add("com.miotech.kun.metadata.web.controller");
        scanList.add("com.miotech.kun.workflow.web.controller");
        scanList.add("com.miotech.kun.infra");
        return new BasePackages(scanList);
    }

    @Override
    protected void configure() {
        super.configure();
        bind(InitService.class);
    }
}
