import pytest
import re
import pandas as pd
import numpy as np
import datetime
from src.weather_process import WethearMetadata



@pytest.fixture
def main_instance_history():
    return {'location': {'name': 'Moscow','region': 'Moscow City','country': 'Russia','lat': 55.75,'lon': 37.62,\
                         'tz_id': 'Europe/Moscow','localtime_epoch': 1692202248,'localtime': '2023-08-16 19:10'},
             'forecast': {'forecastday': [{'date': '2023-08-15','date_epoch': 1692057600,'day': {'maxtemp_c': 26.9,
                'maxtemp_f': 80.4,'mintemp_c': 17.0,'mintemp_f': 62.6,'avgtemp_c': 21.5,'avgtemp_f': 70.7,'maxwind_mph': 12.5,\
                'maxwind_kph': 20.2,'totalprecip_mm': 0.0,'totalprecip_in': 0.0,'avgvis_km': 10.0,'avgvis_miles': 6.0,\
                'avghumidity': 61.0,'condition': {'text': 'Partly cloudy','icon': '//cdn.weatherapi.com/weather/64x64/day/116.png',\
                'code': 1003},'uv': 7.0},'astro': {'sunrise': '05:01 AM','sunset': '08:06 PM','moonrise': '03:02 AM',\
                'moonset': '08:25 PM','moon_phase': 'Waning Crescent','moon_illumination': '2'},'hour': [{'time_epoch': 1692046800,
                'time': '2023-08-15 00:00','temp_c': 17.8,'temp_f': 64.0,'is_day': 0,'condition': {'text': 'Clear',\
                'icon': '//cdn.weatherapi.com/weather/64x64/night/113.png','code': 1000},'wind_mph': 4.5,'wind_kph': 7.2,\
                'wind_degree': 287,'wind_dir': 'WNW','pressure_mb': 1019.0,'pressure_in': 30.09,'precip_mm': 0.0,\
                'precip_in': 0.0,'humidity': 83,'cloud': 8,'feelslike_c': 17.8,'feelslike_f': 64.0,'windchill_c': 17.8,\
                'windchill_f': 64.0,'heatindex_c': 17.8,'heatindex_f': 64.0,'dewpoint_c': 14.9,'dewpoint_f': 58.8,\
                'will_it_rain': 0,'chance_of_rain': 0,'will_it_snow': 0,'chance_of_snow': 0,'vis_km': 10.0,'vis_miles': 6.0,\
                'gust_mph': 9.4,'gust_kph': 15.1,'uv': 1.0},{'time_epoch': 1692050400,'time': '2023-08-15 01:00',\
                'temp_c': 17.6,'temp_f': 63.7,'is_day': 0,'condition': {'text': 'Clear','icon': '//cdn.weatherapi.com/weather/64x64/night/113.png',\
                'code': 1000},'wind_mph': 4.5,'wind_kph': 7.2,'wind_degree': 294,'wind_dir': 'WNW','pressure_mb': 1019.0,\
                'pressure_in': 30.09,'precip_mm': 0.0,'precip_in': 0.0,'humidity': 83,'cloud': 9,'feelslike_c': 17.6,\
                'feelslike_f': 63.7,'windchill_c': 17.6,'windchill_f': 63.7,'heatindex_c': 17.6,'heatindex_f': 63.7,\
                'dewpoint_c': 14.7,'dewpoint_f': 58.5,'will_it_rain': 0,'chance_of_rain': 0,'will_it_snow': 0,\
                'chance_of_snow': 0,'vis_km': 10.0,'vis_miles': 6.0,'gust_mph': 9.4,'gust_kph': 15.1,'uv': 1.0}]}]}}

@pytest.fixture
def main_instance_forecast():
    return {'location': {'name': 'London','region': 'City of London, Greater London','country': 'United Kingdom','lat': 51.52,\
                         'lon': -0.11,'tz_id': 'Europe/London','localtime_epoch': 1692203674,'localtime': '2023-08-16 17:34'},
            'current': {'last_updated_epoch': 1692203400,'last_updated': '2023-08-16 17:30','temp_c': 24.0,'temp_f': 75.2,\
                        'is_day': 1,'condition': {'text': 'Sunny','icon': '//cdn.weatherapi.com/weather/64x64/day/113.png',\
                        'code': 1000},'wind_mph': 9.4,'wind_kph': 15.1,'wind_degree': 100, 'wind_dir': 'E','pressure_mb': 1019.0,\
                        'pressure_in': 30.09,'precip_mm': 0.0,'precip_in': 0.0,'humidity': 44,'cloud': 0,'feelslike_c': 25.1,\
                        'feelslike_f': 77.3,'vis_km': 10.0,'vis_miles': 6.0,'uv': 6.0,'gust_mph': 8.3,'gust_kph': 13.3},\
            'forecast': {'forecastday': [{'date': '2023-08-16','date_epoch': 1692144000,'day': {'maxtemp_c': 24.9,\
                        'maxtemp_f': 76.8,'mintemp_c': 15.5,'mintemp_f': 59.9,'avgtemp_c': 19.8,'avgtemp_f': 67.6,\
                        'maxwind_mph': 7.2,'maxwind_kph': 11.5,'totalprecip_mm': 0.0,'totalprecip_in': 0.0,'totalsnow_cm': 0.0,\
                        'avgvis_km': 10.0,'avgvis_miles': 6.0,'avghumidity': 63.0,'daily_will_it_rain': 0,'daily_chance_of_rain': 0,\
                        'daily_will_it_snow': 0,'daily_chance_of_snow': 0,'condition': {'text': 'Partly cloudy',\
                        'icon': '//cdn.weatherapi.com/weather/64x64/day/116.png','code': 1003},'uv': 5.0},'astro': {'sunrise': '05:47 AM',\
                        'sunset': '08:21 PM','moonrise': '05:20 AM','moonset': '08:53 PM','moon_phase': 'New Moon',\
                        'moon_illumination': '0','is_moon_up': 0,'is_sun_up': 0},'hour': [{'time_epoch': 1692140400,\
                        'time': '2023-08-16 00:00','temp_c': 17.1, 'temp_f': 62.8,'is_day': 0,'condition': {'text': 'Partly cloudy',\
                        'icon': '//cdn.weatherapi.com/weather/64x64/night/116.png','code': 1003},'wind_mph': 2.5,'wind_kph': 4.0,\
                        'wind_degree': 270,'wind_dir': 'W','pressure_mb': 1019.0,'pressure_in': 30.09,'precip_mm': 0.0,\
                        'precip_in': 0.0,'humidity': 79,'cloud': 32,'feelslike_c': 17.1,'feelslike_f': 62.8,'windchill_c': 17.1,\
                        'windchill_f': 62.8,'heatindex_c': 17.1,'heatindex_f': 62.8,'dewpoint_c': 13.4,'dewpoint_f': 56.1,\
                        'will_it_rain': 0,'chance_of_rain': 0,'will_it_snow': 0,'chance_of_snow': 0,'vis_km': 10.0,\
                        'vis_miles': 6.0,'gust_mph': 4.5,'gust_kph': 7.2,'uv': 1.0}, {'time_epoch': 1692144000,\
                        'time': '2023-08-16 01:00','temp_c': 16.8,'temp_f': 62.2,'is_day': 0,'condition': {'text': 'Clear',\
                        'icon': '//cdn.weatherapi.com/weather/64x64/night/113.png','code': 1000},'wind_mph': 2.2,\
                        'wind_kph': 3.6,'wind_degree': 287,'wind_dir': 'WNW','pressure_mb': 1019.0,'pressure_in': 30.09,\
                        'precip_mm': 0.0,'precip_in': 0.0,'humidity': 79,'cloud': 24,'feelslike_c': 16.8,'feelslike_f': 62.2,\
                        'windchill_c': 16.8,'windchill_f': 62.2,'heatindex_c': 16.8,'heatindex_f': 62.2,'dewpoint_c': 13.2,\
                        'dewpoint_f': 55.8,'will_it_rain': 0,'chance_of_rain': 0,'will_it_snow': 0,'chance_of_snow': 0,\
                        'vis_km': 10.0,'vis_miles': 6.0,'gust_mph': 4.0,'gust_kph': 6.5,'uv': 1.0}]}]}}
#------------------------------------------------------------------------------------------------------------------

def test__main_history_preprocessing(main_instance_history):
    
    wm = WethearMetadata(main_instance_history)

    assert wm.main_instance is True
    assert wm.df_main.shape[1] == 44
    assert wm.df_main.shape[0] == 2
    

def test__main_forecast_preprocessing(main_instance_forecast):
    
    wm = WethearMetadata(main_instance_forecast)

    assert wm.main_instance is True
    assert wm.df_main.shape[1] == 69
    assert wm.df_main.shape[0] == 2
    

def test__apply_map_geo(main_instance_history, col_: str = 'map_geo'):
    
    wm = WethearMetadata(main_instance_history)
    load = wm._apply_maps(col_)

    assert wm.main_instance is True
    assert load is True
    assert not wm.data[col_].empty
    assert wm.data[col_].shape == (1,2)
    

def test__apply_map_condition(main_instance_forecast, col_: str = 'map_condition'):
    
    wm = WethearMetadata(main_instance_forecast)
    load = wm._apply_maps(col_)

    assert wm.main_instance is True
    assert load is True
    assert not wm.data[col_].empty
    assert wm.data[col_].shape[0] > 0
    assert wm.data[col_].shape[1] == 2


def test__apply_weather_preprocessing(main_instance_history):
    
    wm = WethearMetadata(main_instance_history)
    load = wm._apply_weather_preprocessing()
    
    assert wm.main_instance is True
    assert load is True
    assert not wm.data[wm.feature].empty
    assert wm.data[wm.feature].shape[0] == 2
    assert set(wm.rename_columns['weather'].values()) & set(list(wm.data[wm.feature])) == set(list(wm.data[wm.feature]))