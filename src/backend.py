from celery import Celery
import celery as clr
from celery.result import AsyncResult

from src.settings import *
from src.weather_process import WethearMetadata, processing_data_and_uploading
from src.utils import *

from flask import (
                    Flask,
                    Response,
                    render_template,
                    request,
                    json,
                    jsonify,
                    make_response
                    )

app = Flask(__name__, static_url_path='')

#Initialize celery --------------------------------------------------------------------
celery = Celery(app.name)
celery.conf.update(
    result_backend = CELERY_RESULT_BACKEND,
    broker_url = CELERY_BROKER_URL,
    timezone = CELERY_TIMEZONE,
    task_serializer = CELERY_TASK_SERIALIZER,
    accept_content = CELERY_ACCEPT_CONTENT,
    result_serializer = CELERY_RESULT_SERIALIZER,
    beat_schedule = CELERYBEAT_SCHEDULE,
    tasks_routes = CELERY_TASK_ROUTES,
)

#########################################################################################

## logging instance -----------------------------------------------------------------------

logger = logger_init(location = 'init_backendapi', log_path = main_dirs['LOG'])

### DB instance ----------------------------------------------------------------------------

db = DBpostgreSQL(
    db_name = configs['PG__DBNAME'],
    host = configs['PG__HOST'],
    username = configs['PG__USER_BOT'],
    password= configs['PG__PASSW_BOT'], 
    tz = configs['TZ'], 
)
db.logger = logger

############################################################################################
### tasks with delay execution #############################################################
############################################################################################

@celery.task(name = 'celery.upload_history', queue=CELERY_QUEUE_HIST)
def upload_history_post(querystring, dates):
    """
    procedure of async running task for downloading data from EXT API endpoint 'history'
    :dates - the list of isoformat the period of dates like '%Y-%m-%d'
    """
    
    logger = logger_init(location = 'init_backendapi_post', log_path = main_dirs['LOG'])
    db.logger = logger
    key = "{}_{}_{}".format(
        socket.gethostname(),
        date.today().strftime('%Y%m%d'),
        'upload_history',
    )
    ## DB Redis instance ###################################################################
    db_redis = DBredis(
        topic = configs['REDIS_DB_STATUS'],
        host = configs['REDIS_HOST'],
        port = configs['REDIS_PORT'],
    )
    ########################################################################################
    go_on = db_redis.publish_message(key, 'busy')
    if go_on:
        if querystring == {}: querystring = querystring_template['history']
        log(logger, 'API init', 'info', f"Starting with next params:{querystring}, date range = {dates}")
        #### GET API data ------------------------------------------------------------------
        for date_ in dates:
            querystring['dt'] = date_
            go_on = False
            try:
                response = requests.get(configs['WEATHER_API__HISTORY'],\
                                        headers=headers, \
                                        params=querystring)
                if response.status_code == 200: 
                    go_on = True
                else:
                    log(logger, 'API request', 'error', f"date - {querystring['dt']}: {response.text}")
                    continue
            except Exception as err:
                traceback_info = traceback.format_exc()
                log(logger, 'API request', 'error', f"{configs['WEATHER_API__HISTORY']}:{traceback_info}")
                continue
            
            ################################################################################
            processing_data_and_uploading(logger, db, configs, response)
            ################################################################################
    else:
        log(logger, 'API init', 'error', "Could not publish the status")
        
    _ = db_redis.publish_message(key, 'free')


@celery.task(name = 'celery.upload_forecast', queue=CELERY_QUEUE_FORE)
def upload_forecast_post(querystring):
    """
    procedure of async running task for downloading data from EXT API endpoint 'forecast'
    :days - the number of days for the forecast - max value is 6
    """
    
    logger = logger_init(location = 'init_backendapi_post', log_path = main_dirs['LOG'])
    db.logger = logger
    key = "{}_{}_{}".format(
        socket.gethostname(),
        date.today().strftime('%Y%m%d'),
        'upload_forecast',
    )
    ## DB Redis instance ###################################################################
    db_redis = DBredis(
        topic = configs['REDIS_DB_STATUS'],
        host = configs['REDIS_HOST'],
        port = configs['REDIS_PORT'],
    )
    ########################################################################################
    go_on = db_redis.publish_message(key, 'busy')
    if go_on:
        if querystring == {}: querystring = querystring_template['forecast']
        log(logger, 'API init', 'info', f"Starting with next params:{querystring}")
        #### GET API data ------------------------------------------------------------------
        try:
            response = requests.get(configs['WEATHER_API__FORECAST'],\
                                        headers=headers, \
                                        params=querystring)
            if response.status_code == 200: 
                go_on = True
            else:
                log(logger, 'API request', 'error', f"date - {querystring['dt']}: {response.text}")
        except Exception as err:
            traceback_info = traceback.format_exc()
            log(logger, 'API request', 'error', f"{configs['WEATHER_API__FORECAST']}:{traceback_info}")
    else:
        log(logger, 'API init', 'error', "Could not publish the status")
    
    if go_on:
        ################################################################################
        processing_data_and_uploading(logger, db, configs, response)
        ################################################################################
        
    _ = db_redis.publish_message(key, 'free')

####### app routes #########################################################################

@app.route("/tasks", methods=["POST"])
def run_task():
    """
    running celery tasks by link 
    necessary inputs data: {
                            'location': '',
                            'days':3, OR 'dates':['1970-01-01','1970-01-02'],
                            'token': '',
                            'task_type':''
                           }
    """

    db_redis = DBredis(
        topic = configs['REDIS_DB_STATUS'],
        host = configs['REDIS_HOST'],
        port = configs['REDIS_PORT'],
    )
    #---------------------------- parsing post data ---------------------------------------
    request_data = request.get_json()
    if 'querystring' not in request_data:
        return make_response(jsonify({'error':'Bad request: Specify querystring'}), 400)
    if 'task_type' not in request_data:
        return make_response(jsonify({'error':'Bad request: Specify task_type'}),400)
    if 'token' not in request_data:
        return make_response(jsonify({'error':'Bad request: Specify access token'}),400)
    
    #---------------------------- checking auth token -------------------------------------
    if request_data['token'] != configs['API__TOKEN']:
        return make_response(jsonify({'error': 'Invalid access token'}), 401)
    
    #--------------------------- checking status of task in REDIS -------------------------
    key = "{}_{}_{}".format(
        socket.gethostname(),
        date.today().strftime('%Y%m%d'),
        request_data['task_type']
    )
    msg = db_redis.reading_message(key)
    queue_free = True if str(msg[key]) in ['None','free','0'] else False
    if not queue_free:
        return make_response(jsonify({'warning': 'task in processing'}), 201)
    
    #-------------------------- lanching if task_type in allowed methods -----------------
    if request_data['task_type'] == 'upload_history':
        #----------------------processing querystring params -----------------------------
        if request_data['querystring'] == {}:
            querystring = querystring_template['history']
        else:
            querystring = request_data['querystring']
        #---------------------- processing location param --------------------------------
        if 'location' in request_data:
            querystring['q'] = request_data['location']
        #---------------------- processing date range params -----------------------------
        if 'dates' not in request_data:
            dates = [date.today().isoformat()]
        else:
            dates = request_data['dates']
        #---------------------- start delay task -----------------------------------------
        taskss = upload_history_post.delay(querystring, dates,)
        
        return make_response(jsonify( { 'task_id': taskss.id } ), 202)
    ######################################################################################
    elif request_data['task_type'] == 'upload_forecast':
        #----------------------processing querystring params -----------------------------
        if request_data['querystring'] == {}:
            querystring = querystring_template['forecast']
        else:
            querystring = request_data['querystring']
        #---------------------- processing location param --------------------------------
        if 'location' in request_data:
            querystring['q'] = request_data['location']
        #---------------------- processing date range params -----------------------------
        if 'days' not in request_data:
            querystring['days'] = 3
        else:
            querystring['days'] = min(4,request_data['days'])
        #---------------------- start delay task -----------------------------------------
        taskss = upload_forecast_post.delay(querystring,)
        
        return make_response(jsonify( { 'task_id': taskss.id } ), 202)
    ######################################################################################
    else:
        return make_response(jsonify( { 'error': 'not allowed method' } ), 405)
    

@app.route("/tasks/<task_id>", methods=["GET"])
def get_status(task_id):
    """
    Checking status for task by task.id
    """

    task_result = AsyncResult(task_id)
    result = {
        "task_id": task_id,
        "task_status": task_result.status,
        "task_result": task_result.result
    }

    return make_response(jsonify( { 'current_task_status': result } ), 200)

##########################################################################################


if __name__ == "__main__":
    app.run(host=configs['APP__HOST'], port=configs['API__EXT_PORT'], debug = False)