package com.miotech.kun.metadata.model;

import com.miotech.kun.metadata.constant.DatabaseType;

public abstract class QueryEngine {

    private final Type type;

    public QueryEngine(Type type) {
        this.type = type;
    }

    public Type getType() {
        return type;
    }

    @Override
    public abstract String toString();

    public enum Type {
        Athena,
        Presto,
        Hive
    }

    public static String[] parseConnInfos(QueryEngine queryEngine) {
        String[] connInfos = new String[3];
        switch (queryEngine.getType()) {
            case Athena:
                AthenaQueryEngine athenaQueryEngine = (AthenaQueryEngine) queryEngine;
                connInfos[0] = athenaQueryEngine.getUrl();
                connInfos[1] = athenaQueryEngine.getUsername();
                connInfos[2] = athenaQueryEngine.getPassword();
                return connInfos;
            default:
                throw new RuntimeException("Invalid QueryEngine.Type: " + queryEngine.getType());
        }
    }

    public static DatabaseType parseDatabaseTypeFromDataSource(QueryEngine queryEngine) {
        switch (queryEngine.getType()) {
            case Hive:
                return DatabaseType.HIVE;
            case Athena:
                return DatabaseType.ATHENA;
            case Presto:
                return DatabaseType.PRESTO;
            default:
                throw new IllegalArgumentException("Invalid QueryEngine.Type: " + queryEngine.getType());
        }
    }

}
