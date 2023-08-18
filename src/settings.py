import os
from dotenv import load_dotenv

load_dotenv()

main_dirs = {
    'ROOT_DIR': '/app',
    'LOG': '/logs',
    'DATA':'/data',
}

configs = {
    'APP__HOST':os.environ['APP__HOST'],
    'APP__PORT':int(os.environ['APP__PORT']),
    'APP__WORKERS':int(os.environ['APP__WORKERS']),
    'TZ': os.environ['TZ'],
    'PG__PORT': os.environ['PG__PORT'],
    'PG__HOST': os.environ['PG__HOST'],
    'PG__USER_BOT': os.environ['PG__USER_BOT'],
    'PG__PASSW_BOT': os.environ['PG__PASSW_BOT'],
    'PG__DBNAME': os.environ['PG__DBNAME'],
    'PG__SCHEMA': os.environ['PG__SCHEMA'],
    'API__KEY':os.environ['API__KEY'],
    'API__EXT_PORT':os.environ['API__EXT_PORT'],
    'API__TOKEN':os.environ['API__TOKEN'],
    'DB__TABLE_WEATHER':'local_temperature',
    'DB__TABLE_MAP_GEO':'geoid',
    'DB__TABLE_MAP_CONDITION':'conditionid',
    'FILE__STATS':'agg_stats_{id}.csv',
    'WEATHER_API__HISTORY':os.environ['WEATHER_API__HISTORY'],
    'WEATHER_API__CURRENT':os.environ['WEATHER_API__CURRENT'],
    'WEATHER_API__FORECAST':os.environ['WEATHER_API__FORECAST'],
    'WEATHER_API__TOKEN':os.environ['WEATHER_API__TOKEN'],
    'API__INT_POST':os.environ['API__INT_POST'],
    'REDIS_HOST':'127.0.0.1',
    'REDIS_PORT':6379,
    'REDIS_DB':0,
    'REDIS_DB_STATUS':2,
}

### REDIS Settings------------------------------------------------------------------------------------------
REDIS_HOST = configs['REDIS_HOST']
REDIS_PORT = configs['REDIS_PORT']
REDIS_DB = configs['REDIS_DB']
BROKER_URL = os.environ.get('REDIS_URL', f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

### CELERY settings ----------------------------------------------------------------------------------------
CELERY_BROKER_URL = BROKER_URL
CELERY_RESULT_BACKEND = BROKER_URL
CELERY_TIMEZONE = configs['TZ']
CELERY_ACCEPT_CONTENT = ['json', 'msgpack', 'yaml']
CELERY_TASK_SERIALIZER = 'json'
CELERY_RESULT_SERIALIZER = 'json'
CELERY_TASK_ROUTES = {
    'celery.upload_main_stat':{'queue':'main'},
}
CELERYBEAT_SCHEDULE = {}
CELERY_QUEUE_HIST = 'history'
CELERY_QUEUE_FORE = 'forecast'

################################### API settings ###########################################

headers = {
    "X-RapidAPI-Key": configs['WEATHER_API__TOKEN'],
    "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com",
}

querystring_template = {
    'history':{
        "q":"London",
        "dt":"1970-01-01",
        "lang":"en",
    },
    'current':{
        "q":"London",
    },
    'forecast':{
        "q":"London",
        "days":"3",
    },
}

##### URL for triggering async tasks in worker for parsing data
url_int_post = configs['API__INT_POST'].format(**{'port':configs['API__EXT_PORT']})


############################################################################################