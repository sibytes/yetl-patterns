SELECT
  id            ,
  first_name    ,
  last_name     ,
  email         ,
  gender        ,
  job_title     ,
  amount        ,
  allow_contact ,
  current_timestamp() as from_datetime,
  to_timestamp('9999-12-31 23:59:59.999') as to_datetime,
  true as current,
  cast(null as datetime) as deleted_datetime  
FROM {{database_name}}.{{table_name}}
WHERE _TIMESLICE = {{timeslice}}