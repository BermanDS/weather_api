#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<-EOSQL
    
    /* -------------------------------- Create role ------------------------------------------------*/
    -- Create Role: bot_parser 
    CREATE ROLE bot_parser WITH
    LOGIN
    NOSUPERUSER
    INHERIT
    CREATEDB
    NOCREATEROLE
    NOREPLICATION
    ENCRYPTED PASSWORD 'md5bb28a329be706bc0b8994c783541873d';
    COMMENT ON ROLE bot_parser IS 'for parsing data by NBA data';
    /* -------------------- Create database ------------------------------------------------------------*/
    CREATE DATABASE weather
    WITH 
    OWNER = admin
    ENCODING = 'UTF8'
    LC_COLLATE = 'en_US.utf8'
    LC_CTYPE = 'en_US.utf8'
    TABLESPACE = pg_default
    CONNECTION LIMIT = -1;   
    
    /* -------------------------- Create schema ------------------------------------------------------*/
    -- SCHEMA: regional
    \connect weather;
    CREATE SCHEMA regional
    AUTHORIZATION admin;
    GRANT ALL ON SCHEMA regional TO admin;
    GRANT ALL ON SCHEMA regional TO bot_parser;

    /*------------------------- Create tables ---------------------------------------------------------------*/
    -- Table: regional.local_temperature
    CREATE TABLE IF NOT EXISTS regional.local_temperature
    (
    geo_id integer NOT NULL,
    latitude real,
    longitude real,
    datetime timestamp without time zone,
    datetime_update timestamp without time zone,
    temp_c real,
    temp_c_feelslike real,
    cloud integer,
    condition_id integer, 
    humidity integer,
    is_day boolean,
    wind_speed_kph real,
    wind_gust_kph real,
    uv_index real,
    pressure_mb real,
    wind_direction character varying,
    CONSTRAINT regional_local_temperature_pkey PRIMARY KEY (geo_id,datetime)
    )
    TABLESPACE pg_default;
    ALTER TABLE regional.local_temperature OWNER to admin;
    REVOKE ALL ON TABLE regional.local_temperature FROM bot_parser;
    GRANT ALL ON TABLE regional.local_temperature TO admin;
    GRANT DELETE, INSERT, SELECT, UPDATE, TRUNCATE ON TABLE regional.local_temperature TO bot_parser;
    
    CREATE INDEX local_temperature_temp_c_ids_inx
        ON regional.local_temperature USING btree
        (temp_c ASC NULLS LAST) TABLESPACE pg_default;
    CREATE INDEX local_temperature_datetime_inx
        ON regional.local_temperature USING btree
        (datetime ASC NULLS LAST)
    TABLESPACE pg_default;
        
    -- Table: regional.geoid
    CREATE TABLE IF NOT EXISTS regional.geoid
    (
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    geo_name character varying COLLATE pg_catalog."default",
    geo_country character varying COLLATE pg_catalog."default",
    CONSTRAINT geoid_pkey PRIMARY KEY (id)
    )  TABLESPACE pg_default;
    ALTER TABLE regional.geoid OWNER to admin;
    REVOKE ALL ON TABLE regional.geoid FROM bot_parser;
    GRANT ALL ON TABLE regional.geoid TO admin;
    GRANT DELETE, INSERT, SELECT, UPDATE, TRUNCATE ON TABLE regional.geoid TO bot_parser;

    -- Table: regional.conditionid
    CREATE TABLE IF NOT EXISTS regional.conditionid
    (
    id integer NOT NULL,
    condition_str character varying COLLATE pg_catalog."default",
    CONSTRAINT condition_id_pkey PRIMARY KEY (id)
    ) TABLESPACE pg_default;
    ALTER TABLE regional.conditionid OWNER to admin;
    REVOKE ALL ON TABLE regional.conditionid FROM bot_parser;
    GRANT ALL ON TABLE regional.conditionid TO admin;
    GRANT DELETE, INSERT, SELECT, UPDATE, TRUNCATE ON TABLE regional.conditionid TO bot_parser;
    
EOSQL