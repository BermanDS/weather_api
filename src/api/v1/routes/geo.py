from src.logger import *
from fastapi import APIRouter

from src.data.models import (
                            WeatherRequestHistory,
                            WeatherRequestForecast,
                            WeatherResponse
                            )
from src.data.get_data import WeatherDataFromDB, WeatherDataFromAPI
from src.settings import configs

router = APIRouter()

#------------------------------------------------------------------------------------------------
############## the endpoint /weather/history ####################################################
#------------------------------------------------------------------------------------------------

@router.post("/history")
async def history(request: WeatherRequestHistory) -> WeatherResponse:
    """
    Get weather's data on definite city and country for period or date from DB or API
    (if not exist in DB)
    
    Request body:
    - city:    string require (the name of location)
    - country: string optional (the country if necessary to specify it)
    - dates:   string (list of dates in string format like %Y-%m-%d separated by comma)

    Returns:
    - current_time
    - metainfo - about process of retrieving data from DB and/or API
    - data - list of dicts (with 24 values for the definite date)
        - 'datetime':               timestamp (without time zone),
        - 'temperature':            float (value in C degree),
        - 'temperature_feels_like': float (value in C degree),
        - 'condition_weather':      string (description of weather condition),
        - 'uv_index':               float,
        - 'humidity':               float (in %),
        - 'pressure_mb':            float,
        - 'day_or_night':           string ('day' or 'night' values),
        - 'geo_location':           string (the name of location in format - "City, Country")
    """
    
    ### processing request ----------------------------------------------------------------------
    pars = request.get_dict
    log(logger, 'parsing params', 'info', f"Got next input params : {pars}")
    if pars['geo_country'] is None: pars.pop('geo_country')
    pars['condition_geo'] =  (
        ' AND '
        .join([f"LOWER({x[0]}) = " + json.dumps(x[1], cls= NpEncoder).replace('"',"'").lower() \
                for x in pars.items() \
                if x[0] in ['geo_name','geo_country']])
    )
    
    ### trying to retrieve data from DB ---------------------------------------------------------
    wdb = WeatherDataFromDB()
    df = await wdb.get_history_by_dates_and_locations(**pars)
    task_id = None
    ### check - if all requested dates in response, else making request to origin API -----------
    if set(map(parse, df['check_date'].astype(str).unique())) & set(map(parse, pars['date_range'])) \
        != set(map(parse, pars['date_range'])):
        pars['date_range'] = list(
            set(map(lambda x: parse(x).strftime('%Y-%m-%d'), df['check_date'].astype(str).unique())) ^ \
            set(pars['date_range'])
        )
        wapi = WeatherDataFromAPI()
        load, task_id = wapi.trigger_backend_history_by_dates_and_locations(**pars)
        if load:
            metainfo = 'Data was parsed additionally from Weather API'
        else:
            metainfo = 'There are maybe issue with caching data from API'
        #----------------------------------------------------------
        pars['condition_geo'] =  (
            ' AND '
            .join([f"LOWER({x[0]}) LIKE " + json.dumps(x[1]+'%', cls= NpEncoder).replace('"',"'").lower() \
                    for x in pars.items() \
                    if x[0] in ['geo_name','geo_country']])
        )
        df = await wdb.get_history_by_dates_and_locations(**pars)
    else:
        metainfo = 'All data available from cache'
    #--------------------------------------------------------------------------------------------
    
    return WeatherResponse(
        data = df.drop(['check_date'], axis = 1).to_dict(orient = 'records'),
        metainfo = {
            'message': metainfo, 
            'backend_task':task_id,
        },
        current_time = datetime.now().astimezone(pytz.timezone(configs['TZ'])).isoformat()
    )

#------------------------------------------------------------------------------------------------
############## the endpoint /weather/history ####################################################
#------------------------------------------------------------------------------------------------

@router.post("/forecast")
async def forecast(request: WeatherRequestForecast) -> WeatherResponse:
    """
    Get weather's data on definite city and country as forecast for the next 1-3 days 
    from DB or API (if not exist in DB)
    
    Request body:
    - city:    string require (the name of location)
    - country: string optional (the country if necessary to specify it)
    - days:    integer (maximum value is 3)

    Returns:
    - current_time
    - metainfo - about process of retrieving data from DB and/or API
    - data - list of dicts (with 24 values for the definite date)
        - 'datetime':               timestamp (without time zone),
        - 'temperature':            float (value in C degree),
        - 'temperature_feels_like': float (value in C degree),
        - 'condition_weather':      string (description of weather condition),
        - 'uv_index':               float,
        - 'humidity':               float (in %),
        - 'pressure_mb':            float,
        - 'day_or_night':           string ('day' or 'night' values),
        - 'geo_location':           string (the name of location in format - "City, Country")
    """
    
    ### processing request ----------------------------------------------------------------------
    pars = request.get_dict
    log(logger, 'parsing params', 'info', f"Got next input params : {pars}")
    if pars['geo_country'] is None: pars.pop('geo_country')
    pars['condition_geo'] =  (
        ' AND '
        .join([f"LOWER({x[0]}) = " + json.dumps(x[1], cls= NpEncoder).replace('"',"'").lower() \
                for x in pars.items() \
                if x[0] in ['geo_name','geo_country']])
    )
    
    ### trying to retrieve data from DB ---------------------------------------------------------
    wdb = WeatherDataFromDB()
    df = await wdb.get_forecast_by_locations(**pars)
    task_id = None
    
    ### check - if all requested days in response, else making request to origin API -----------
    if df['check_date'].nunique() < pars['days']:
        wapi = WeatherDataFromAPI()
        load, task_id = wapi.trigger_backend_forecast_by_days_and_locations(**pars)
        if load:
            metainfo = 'Data was parsed additionally from Weather API'
        else:
            metainfo = 'There are maybe issue with caching data from API'
        #----------------------------------------------------------
        pars['condition_geo'] =  (
            ' AND '
            .join([f"LOWER({x[0]}) LIKE " + json.dumps(x[1]+'%', cls= NpEncoder).replace('"',"'").lower() \
                    for x in pars.items() \
                    if x[0] in ['geo_name','geo_country']])
        )
        df = await wdb.get_forecast_by_locations(**pars)
    else:
        metainfo = 'All data available from cache'
    #--------------------------------------------------------------------------------------------
    
    return WeatherResponse(
        data = df.drop(['check_date'], axis = 1).to_dict(orient = 'records'),
        metainfo = {
            'message': metainfo, 
            'backend_task':task_id,
        },
        current_time = datetime.now().astimezone(pytz.timezone(configs['TZ'])).isoformat()
    )

#------------------------------------------------------------------------------------------------