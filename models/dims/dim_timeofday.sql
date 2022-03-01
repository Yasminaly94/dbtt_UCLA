{{ config(materialized='table') }}


select 
    TIMEADD(minute, SEQ4(), '2017-01-01 00:00:00') as TEMP_TIMESTAMP,
    seq4() as TEMP_SEQUENCE,
    row_number() over (order by TEMP_SEQUENCE) as TIME_PK,
    to_time(to_char(TEMP_TIMESTAMP,'HH24:MI:00'),'HH24:MI:00') Hour_24,
    to_char(TEMP_TIMESTAMP,'HH12:MI:00 PM') HOUR_12,
    hour(TEMP_TIMESTAMP) as HROFDAY,
    minute(TEMP_TIMESTAMP) as MINOFDAY
    from table(generator(rowcount => 1440))