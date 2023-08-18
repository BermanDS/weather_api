"""Weather API processing module."""
from src.utils import *


class WethearMetadata:
    """Class that splite a weather data to separate instances."""
    
    
    def __init__(self,
                dict_from_response: dict = {}):
        """
        :df_from_json: Main instance of data. from pd.json_normalize
        """

        self.dict_from_response = dict_from_response
        self.df_main = pd.DataFrame()
        self.main_instance = False
        self.data = {}
        self.rename_columns = {
            'weather_now': {
                'location.name':'geo_id','location.country':'geo_country','location.lat':'latitude','location.lon':'longitude',
                'location.localtime':'datetime','current.last_updated':'datetime_update','current.temp_c':'temp_c',
                'current.feelslike_c':'temp_c_feelslike','current.condition.text':'condition_str','current.condition.code':'condition_id',
                'current.wind_kph':'wind_speed_kph','current.gust_kph':'wind_gust_kph','current.uv':'uv_index','is_day':'is_day',
                'current.wind_dir':'wind_direction','current.pressure_mb':'pressure_mb','current.humidity':'humidity','current.cloud':'cloud',
            },
            'weather': {
                'location.name':'geo_id','location.lat':'latitude','location.lon':'longitude','pressure_mb':'pressure_mb',
                'location.localtime':'datetime_update','time':'datetime','temp_c':'temp_c','is_day':'is_day','cloud':'cloud',
                'feelslike_c':'temp_c_feelslike','condition.code':'condition_id','humidity':'humidity','wind_kph':'wind_speed_kph',
                'gust_kph':'wind_gust_kph','uv':'uv_index','wind_dir':'wind_direction',
            },
            'map_geo':{
                'location.name':'geo_name','location.country':'geo_country',
            },
            'map_condition':{
                'condition.text':'condition_str','condition.code':'condition_id',
            },
        }
        self.msg = ''
        self._convert_response_to_df()

    def _convert_response_to_df(self):
        """
        convert dict self.dict_from_response to self.df_main prepared for processing
        """
        
        
        if isinstance(self.dict_from_response, dict):
            df = pd.json_normalize(self.dict_from_response)
            self.df_main = (
                pd
                .json_normalize(
                    flatten_list(
                        pd.json_normalize(
                            df['forecast.forecastday'].values[0]
                        )['hour'].values
                    )
                )
                .join(df)
                .fillna({
                    'location.name':df['location.name'].values[0],
                    'location.country':df['location.country'].values[0],
                    'location.lat':df['location.lat'].values[0],
                    'location.lon':df['location.lon'].values[0],
                    'location.localtime':df['location.localtime'].values[0]
                })
            )
            self.main_instance = not self.df_main.empty
        else:
            self.msg = f'Type of input object missmatch - not json: {type(self.dict_from_response)}'
            self.main_instance = False
            
    
    
    def _apply_maps(self, feature: str = 'map_geo') -> bool:
        """
        Assemble map geo from main instance
        """

        self.feature = feature
        self.data[self.feature] = pd.DataFrame()
        
        if not self.main_instance:
            self.msg = f'Main instance not prepared or empty'
        elif set(list(self.df_main)) & set(self.rename_columns[self.feature].keys()) != \
            set(self.rename_columns[self.feature].keys()):
            self.msg = f'Absent {feature} columns in response'
        else:
            self.data[self.feature] = (
                self.df_main
                .rename(self.rename_columns[self.feature], axis = 1)
                [self.rename_columns[self.feature].values()]
                .drop_duplicates()
            )
        
        return not self.data[self.feature].empty
    

    def _apply_weather_preprocessing(self) -> bool:        
        """
        Apply preprocessing the main instance of data from endpoints - 'history', 'forecast'
        """
        
        self.feature = 'weather'
        self.data[self.feature] = pd.DataFrame()
        
        if not self.main_instance:
            self.msg = f'Main instance not prepared or empty'
        elif set(list(self.df_main)) & set(self.rename_columns[self.feature].keys()) != \
            set(self.rename_columns[self.feature].keys()):
            self.msg = 'absent some key columns in response'
        else:
            self.data[self.feature] = (
                self.df_main
                .rename(self.rename_columns[self.feature], axis = 1)
                .astype({
                    'latitude':float,
                    'longitude':float,
                    'temp_c_feelslike':float,
                    'temp_c':float,
                    'wind_speed_kph':float,
                    'uv_index':float,
                    'pressure_mb':float,
                    'humidity':int,
                    'cloud':int,
                    'condition_id':int,
                    'is_day':bool
                })
                [self.rename_columns[self.feature].values()]
            )
        
        return not self.data[self.feature].empty

#########################################################################################################

def processing_data_and_uploading(logger: object = None, db: object = None, configs: dict = None, response: object = None) -> None:
    """
    functionality of processing weather data for history and forecast endpoints
    """
    
    dfw = WethearMetadata(response.json())
    
    go_on = dfw.main_instance
    if not go_on:
        log(logger, 'processing main instance', 'error', dfw.msg)
        
    ### processing table GEOID -----------------------------------------------------
    col_ = 'map_geo'
    table_db = configs['PG__SCHEMA']+'.'+configs[f"DB__TABLE_{col_.upper()}"]
    
    if not go_on:
        None
    elif dfw._apply_maps(col_):
        log(logger, f'processing map: {col_}', 'info', str(dfw.data[col_].shape))
        go_on, _ = db.insert_if_not_exist(
            table = table_db,
            values = dfw.data[col_].to_dict('records')[0]
        )
    else:
        log(logger, f'processing map: {col_}', 'error', dfw.msg)
        go_on = False
                
    if go_on:
        log(logger, f'processing map: {col_}', 'info', 'trying to get id for geo')
        
        geo_id_data, go_on = db.select_rows(
            query = f"SELECT id FROM {table_db}", 
            condition = dfw.data[col_].to_dict('records')[0]
        )
                
    ########### ---------------------------------------------------------------------
    
    ### processing table CONDITIONID ------------------------------------------------
    col_ = 'map_condition'
    table_db = configs['PG__SCHEMA']+'.'+configs[f"DB__TABLE_{col_.upper()}"]
    
    if not go_on:
        None
    elif dfw._apply_maps(col_):
        log(logger, f'processing map: {col_}', 'info', str(dfw.data[col_].shape))
        go_on = db.update_values(
            df = (
                dfw
                .data[col_]
                .rename({'condition_id':'id'}, axis = 1)
                .sort_values('condition_str')
                .drop_duplicates(['id'])
            ),
            table = table_db,
            feat_pk = 'id'
        )
    else:
        log(logger, f'processing map: {col_}', 'error', dfw.msg)
        go_on = False
                
    ###############-----------------------------------------------------------------
    
    ### processing MAIN DATA -------------------------------------------------------
    if not go_on:
        None
    elif dfw._apply_weather_preprocessing():
        log(logger, f'processing main table', 'info', str(dfw.data[dfw.feature].shape))
        ### mapping geo_id ---------------------------------------------------------
        dfw.data[dfw.feature]['geo_id'] = geo_id_data[0][0]
        ### saving to DB main DF ---------------------------------------------------
        go_on = db.update_values(
            df = dfw.data[dfw.feature],
            table = f"{configs['PG__SCHEMA']}.{configs['DB__TABLE_WEATHER']}",
            feat_pk = 'geo_id,datetime'
        )
    else:
        log(logger, f'processing main table', 'error', dfw.msg)
        go_on = False
                
    if not go_on:
        log(logger, 'processing instance', 'error',  f"{date_}: uncorrect")
        
    #--------------------------------------------------------------------------------
    gc.collect()