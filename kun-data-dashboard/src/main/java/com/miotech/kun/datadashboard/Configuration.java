package com.miotech.kun.datadashboard;

import com.miotech.kun.commons.utils.PropertyUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

/**
 * @author: Jie Chen
 * @created: 2020/7/19
 */
@org.springframework.context.annotation.Configuration("DataDashboardConfiguration")
public class Configuration {

    @Autowired
    private Environment environment;

    @PostConstruct
    public void setUp() {
        String env = this.environment.getActiveProfiles()[0];
        System.setProperty(PropertyUtils.APP_CONFIG_FILE, String.format("application-%s.yaml", env));
    }
}
