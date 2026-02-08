CREATE TABLE IF NOT EXISTS stg.bonussystem_events (
    id int4 NOT NULL,
    event_ts timestamp NOT NULL,
    event_type varchar NOT NULL,
    event_value jsonb NOT NULL,
    CONSTRAINT bonussystem_events_pkey PRIMARY KEY (id)
);