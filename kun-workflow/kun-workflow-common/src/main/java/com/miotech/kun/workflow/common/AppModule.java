package com.miotech.kun.workflow.common;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
import com.miotech.kun.workflow.common.resource.ResourceLoader;
import com.miotech.kun.workflow.common.resource.ResourceLoaderImpl;

import java.util.Properties;

public abstract class AppModule extends AbstractModule {
    private final Properties props;

    public AppModule(Properties props) {
        this.props = props;
    }

    @Override
    protected void configure() {
        Names.bindProperties(binder(), props);
        bind(Properties.class).toInstance(props);
        bind(ResourceLoader.class).to(ResourceLoaderImpl.class);
    }
}
