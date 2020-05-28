CREATE DOMAIN IF NOT EXISTS "JSONB" AS TEXT;

DROP TABLE IF EXISTS kun_wf_operator;

DROP TABLE IF EXISTS kun_wf_task;

DROP TABLE IF EXISTS kun_wf_tick_task_mapping;

CREATE TABLE kun_wf_operator (
    id BIGINT PRIMARY KEY,
    name VARCHAR(128) UNIQUE,
    description VARCHAR(16384) NOT NULL,
    params JSONB NOT NULL,
    class_name VARCHAR(16384) NOT NULL,
    package VARCHAR(16384) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE kun_wf_task (
    id BIGINT PRIMARY KEY,
    name VARCHAR(1024) NOT NULL,
    description VARCHAR(16384) NOT NULL,
    operator_id BIGINT NOT NULL,
    arguments JSONB NOT NULL,
    variable_defs JSONB NOT NULL,
    schedule JSONB NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

CREATE TABLE kun_wf_tick_task_mapping (
    scheduled_tick VARCHAR(64) NOT NULL,
    task_id BIGINT NOT NULL,
    PRIMARY KEY (scheduled_tick, task_id)
);
