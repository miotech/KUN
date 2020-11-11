package com.miotech.kun.workflow.web;

import com.google.inject.Inject;
import com.google.inject.Injector;
import com.miotech.kun.commons.db.DatabaseModule;
import com.miotech.kun.commons.db.GraphDatabaseModule;
import com.miotech.kun.commons.testing.GuiceTestBase;
import com.miotech.kun.commons.utils.ExceptionUtils;
import com.miotech.kun.commons.utils.Props;
import com.miotech.kun.commons.utils.PropsUtils;
import com.miotech.kun.workflow.SchedulerModule;
import com.miotech.kun.workflow.common.constant.ConfigurationKeys;
import com.miotech.kun.workflow.core.event.Event;
import com.miotech.kun.workflow.core.publish.EventPublisher;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.*;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.junit.ClassRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Neo4jContainer;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class KunWebServerTestBase extends GuiceTestBase {

    private final Logger logger = LoggerFactory.getLogger(KunWebServerTestBase.class);

    @ClassRule
    public static Neo4jContainer neo4jContainer = new Neo4jContainer("neo4j:3.5.20")
            .withAdminPassword("Mi0tech2020");

    @Inject
    private KunWorkflowWebServer webServer;

    @Inject
    private Props props;

    @Override
    protected void configuration() {
        super.configuration();
        Props props = PropsUtils.loadAppProps("application-test.yaml");
        addModules(new KunWorkflowServerModule(props),
                new DatabaseModule(),
                new SchedulerModule(),
                new GraphDatabaseModule(
                        neo4jContainer.getBoltUrl(),
                        "neo4j",
                        neo4jContainer.getAdminPassword())
        );
        bind(EventPublisher.class, new NopEventPublisher());
    }

    @Override
    protected void beforeInject(Injector injector) {
        // initialize database
        setUp(injector);
    }

    public void setUp(Injector injector) {
        props = injector.getInstance(Props.class);
        KunWorkflowWebServer.configureDB(injector,props);
        webServer = injector.getInstance(KunWorkflowWebServer.class);
        new Thread(() -> webServer.start()).start();
        while(!webServer.isReady()) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                throw ExceptionUtils.wrapIfChecked(e);
            }
        }
    }

    @After
    public void tearDown() {
        webServer.shutdown();
    }

    protected String post(String url, String message){
        HttpPost postRequest = new HttpPost(buildUrl(url));
        try {
            postRequest.setEntity(new StringEntity(message));
            return request(postRequest);
        } catch (UnsupportedEncodingException e) {
            throw ExceptionUtils.wrapIfChecked(e);
        }
    }

    protected String put(String url, String message){
        HttpPut putRequest = new HttpPut(buildUrl(url));
        try {
            putRequest.setEntity(new StringEntity(message));
            return request(putRequest);
        } catch (UnsupportedEncodingException e) {
            throw ExceptionUtils.wrapIfChecked(e);
        }
    }

    protected String get(String url) {
        return request(new HttpGet(buildUrl(url)));
    }

    protected String delete(String url) {
        return request(new HttpDelete(buildUrl(url)));
    }

    protected String buildUrl(String url) {
        String port = props.getString(ConfigurationKeys.PROP_SERVER_PORT, "8088");
        if (!url.startsWith("/")) {
            url = "/" + url;
        }
        return "http://localhost:" + port + url;
    }

    private String request(HttpUriRequest request) {
        try {
            HttpResponse responseEntity = HttpClientBuilder.create().build().execute(request);
            String response = EntityUtils.toString(responseEntity.getEntity());
            int statusCode = responseEntity.getStatusLine().getStatusCode();

            if ( statusCode < 200 ||
                    statusCode >= 400 ) {
                logger.error("Http ERROR: " +  response);
            }
            return response;
        } catch (IOException e) {
            logger.error("", e);
            throw ExceptionUtils.wrapIfChecked(e);
        }
    }

    private static class NopEventPublisher implements EventPublisher {
        @Override
        public void publish(Event event) {
            // nop
        }
    }
}
