QUERY_HISTORY_DATES = \
    """WITH geo as (
            SELECT id
                 , geo_name
                 , geo_country
            FROM regional.geoid
            WHERE {condition_geo}
            ORDER BY geo_name, geo_country
            LIMIT 1),
        meta as (
            SELECT geo.id
                 , geo.geo_name
                 , geo.geo_country
                 , lt.datetime::date as date_ 
                 , count(*)
            FROM regional.local_temperature lt, geo 
            WHERE lt.geo_id = geo.id 
            AND datetime_update > lt.datetime  
            GROUP BY 1,2,3,4
            HAVING count(*) = 24)
        SELECT lt.datetime
             , lt.temp_c as temperature
             , lt.temp_c_feelslike as temperature_feels_like
             , cd.condition_str as condition_weather
             , lt.uv_index
             , lt.humidity
             , lt.pressure_mb
             , case when lt.is_day is true then 'day' 
                 else 'night' end as day_or_night
             , meta.geo_name || ', ' || meta.geo_country as geo_location
             , lt.datetime::date as check_date
        FROM regional.local_temperature lt
           , regional.conditionid cd
           , meta
        WHERE meta.id = lt.geo_id
        AND cd.id = lt.condition_id
        AND meta.date_ = lt.datetime::date
        AND meta.date_ in ('{dates}')
        ORDER BY 1;"""
#--------------------------------------------------------------------------

QUERY_FORECAST_DATES = \
    """WITH geo as (
            SELECT id
                 , geo_name
                 , geo_country
            FROM regional.geoid
            WHERE {condition_geo}
            ORDER BY geo_name, geo_country
            LIMIT 1),
        meta as (
            SELECT geo.id
                 , geo.geo_name
                 , geo.geo_country
                 , lt.datetime as date_ 
            FROM regional.local_temperature lt, geo 
            WHERE lt.geo_id = geo.id 
            AND lt.datetime::timestamp > CURRENT_TIMESTAMP)
        SELECT lt.datetime
             , lt.temp_c as temperature
             , lt.temp_c_feelslike as temperature_feels_like
             , cd.condition_str as condition_weather
             , lt.uv_index
             , lt.humidity
             , lt.pressure_mb
             , case when lt.is_day is true then 'day' 
                 else 'night' end as day_or_night
             , meta.geo_name || ', ' || meta.geo_country as geo_location
             , lt.datetime::date as check_date
        FROM regional.local_temperature lt
           , regional.conditionid cd
           , meta
        WHERE meta.id = lt.geo_id
        AND cd.id = lt.condition_id
        AND meta.date_ = lt.datetime
        ORDER BY 1;"""