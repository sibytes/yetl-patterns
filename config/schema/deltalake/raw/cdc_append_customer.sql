CREATE TABLE {{database_name}}.{{table_name}}
(
    extract_date        timestamp not null,
    load_flag           string not null,
    id                  integer not null,
    first_name          string not null,
    last_name           string,
    email               string,
    gender              string,
    job_title           string,
    amount              double,
    _correlation_id     string not null,
    _load_timestamp     timestamp not null,
    _filename           string
)
USING DELTA LOCATION '{{path}}';
