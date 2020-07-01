package com.miotech.kun.commons.web.module;

import com.google.inject.AbstractModule;
import com.miotech.kun.workflow.db.DatabaseModule;

public class CommonModule extends AbstractModule {
    @Override
    protected void configure() {
        install(new DatabaseModule());
    }
}
