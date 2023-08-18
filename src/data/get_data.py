import asyncpg
from src.utils import *
from src.logger import logger
from src.settings import configs, url_int_post
from src.queries import (
                        QUERY_HISTORY_DATES, 
                        QUERY_FORECAST_DATES,
                        )


#-------------------------------------------------------------------------------------------------
############################## Request to backend DB for retrieving cached data ------------------
#-------------------------------------------------------------------------------------------------

class WeatherDataFromDB:
    
    
    @staticmethod
    async def _fetch_as_df(query: str, *args) -> pd.DataFrame:
        
        log(logger, "data from DB", 'info', "Got query : "+query.replace('\n', ' ').strip())
        exc = None
        conn = await asyncpg.connect(
              host=configs['PG__HOST'],
              port=configs['PG__PORT'],
              user=configs['PG__USER_BOT'],
              password=configs['PG__PASSW_BOT'],
              database=configs['PG__DBNAME'],
            )

        try:
            stmt = await conn.prepare(query)
            columns = [a.name for a in stmt.get_attributes()]
            data = await stmt.fetch(*args)
            return pd.DataFrame(data, columns=columns)
        except Exception as e:
            exc = e
        finally:
            await conn.close()
            if exc:
                raise exc

    async def get_history_by_dates_and_locations(self, **pars_) -> object:
        geo_info = await self._fetch_as_df(
            query=QUERY_HISTORY_DATES.format(**pars_)
        )
        
        return geo_info
    
    
    async def get_forecast_by_locations(self, **pars_) -> object:
        geo_info = await self._fetch_as_df(
            query=QUERY_FORECAST_DATES.format(**pars_)
        )
        
        return geo_info

#-------------------------------------------------------------------------------------------------
############################## Request to backend API for caching necessary data to DB -----------
#-------------------------------------------------------------------------------------------------

class WeatherDataFromAPI:
    
    
    def _run_backend_task(self, json_body: str, url: str) -> (bool, str):
        
        log(logger, "data from API", 'info', f"Got body for request : {json_body}, for URL: {url}")
        response = requests.post(url, json = json_body)
        
        if response.status_code == 202:
            task_id = response.json()['task_id']
            log(logger, 'started internal worker', 'info', f"at task_id {task_id}")
                    
            ### defining timeout for parsing API task = 6 sec -------------------------------------
            for _ in range(3):
                ### request worker for checking status of task execution
                response = requests.get(f"{url}/{task_id}")
                if response.status_code == 200:
                    if response.json()['current_task_status']['task_status'] == 'SUCCESS': 
                        break
                else:
                    log(logger, 'checking status internal worker', 'error', \
                        f"for task_id {task_id} : {response.text}")
                    break
                sleep(2)
            return True, task_id
        else:
            log(logger, 'fail with internal worker', 'error',\
                f"Problem with initialization task : {response.text}")
            return False, None
        

    def trigger_backend_history_by_dates_and_locations(self, **pars_) -> bool:
        
        json_body = {
            'querystring':{
                "q": pars_['geo_name'],
                "dt": "",
                "lang":"en",
            },
            'dates':pars_['date_range'],
            'token': configs['API__TOKEN'],
            'task_type':'upload_history',
        }
        success, task_id = self._run_backend_task(json_body, url_int_post)
        
        return success, task_id
    
    
    def trigger_backend_forecast_by_days_and_locations(self, **pars_) -> bool:
        
        json_body = {
            'querystring':{
                "q": pars_['geo_name'],
                "lang":"en",
            },
            'days':pars_['days'],
            'token': configs['API__TOKEN'],
            'task_type':'upload_forecast',
        }
        success, task_id = self._run_backend_task(json_body, url_int_post)
        
        return success, task_id