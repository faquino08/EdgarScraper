import logging
import sys
import datetime

from os import path, environ

import configparser
import requests
import json
import time
from splinter import Browser
from selenium.webdriver.chrome.options import Options

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from flask import Flask, request, g
from flask_restful import Api
from flask_apscheduler import APScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.executors.pool import ThreadPoolExecutor

from constants import POSTGRES_LOCATION, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD, DEBUG
from database import db

from DataBroker.edgar import getFyAndFq, getFyAndFqList, getMissingFilingsIndex, getMissingTickers

# set configuration values
class Config:
    SCHEDULER_API_ENABLED = True

def create_app():
    '''
    Function that creates our Flask application.
    '''
    cache = {}
    if DEBUG:
        logging.basicConfig(
            level=logging.DEBUG,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/EdgarFlask_{datetime.date.today()}.txt'), logging.StreamHandler()],
        )
    else:
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
            datefmt="%m-%d %H:%M",
            handlers=[logging.FileHandler(f'./logs/EdgarFlask_{datetime.date.today()}.txt'), logging.StreamHandler()],
        )
    logger = logging.getLogger(__name__)
    app = Flask(__name__)
    app.config.from_object(Config())
    logger.info("Starting Scheduler")
    db.init_app(app)
    # initialize scheduler
    executors = {
        'default': ThreadPoolExecutor(1),
    }
    background = BackgroundScheduler(executors=executors)
    scheduler = APScheduler(scheduler=background)
    scheduler.init_app(app)
    logger.info(POSTGRES_PORT)
    params = {
            "host": f'{POSTGRES_LOCATION}',
            "port": f'{POSTGRES_PORT}',
            "database": f'{POSTGRES_DB}',
            "user": f'{POSTGRES_USER}',
            "password": f'{POSTGRES_PASSWORD}'
        }
        
    @scheduler.task('cron', id='edgar_missing_entries', minute='30', hour='23', day_of_week='mon-fri', timezone='America/New_York')
    def edgar_scheduledDownload():
        msg = datetime.date.today().year
        getMissingFilingsIndex(params,DEBUG,msg)
        return

    @app.route("/manual_edgar_missing_entries", methods=['POST'])
    def edgar_getMissingFilingsIndex():
        start = int(request.args.get('year',2020))
        endYear = int(request.args.get('end',None))
        logger.info('Start:' + str(start))
        logger.info('End: ' + str(endYear))
        addGetMissingFilingsIndex(scheduler,[params,DEBUG,start,endYear])
        return json.dumps({
            'status':'success',
            'function': 'edgar_missing_entries',
            'res': start,
        })

    @app.route("/manual_edgar_missing_tickers", methods=['POST'])
    def edgar_getMissingTickers():
        msg = request.args.get('date',"2020-01-01")
        addGetMissingTickers(scheduler,[params,DEBUG,msg])
        logger.info(msg)
        return json.dumps({
            'status':'success',
            'function': 'edgar_missing_tickers',
            'res': msg,
        })

    @app.route("/manual_edgar_getfy_fq", methods=['POST'])
    def edgar_getFyAndFq():
        msg = request.args.get('cik',"1390777")
        delay = int(request.args.get('delay',"30"))
        logger.info(msg)
        addGetFyAndFq(scheduler,[params,DEBUG,[msg,delay]])
        return json.dumps({
            'status':'success',
            'function': 'edgar_getfy_fq',
            'res': msg,
        })
    
    @app.route("/manual_edgar_getfy_fq_list", methods=['POST'])
    def edgar_getFyAndFqList():
        getList = list(request.json['ciks'])
        delay = int(request.args.get('delay',"30"))
        logger.info(len(list(getList)))
        addGetFyAndFqList(scheduler,[params,DEBUG,[getList,delay]])
        return json.dumps({
            'status':'success',
            'function': 'edgar_getFyAndFqList',
            'res': len(list(getList)),
        })

    scheduler.start()
    return app

def addGetFyAndFq(scheduler=APScheduler,args=[]):
    '''
    Add paring 10-K and 10-Q flow to AP Scheduler.
    scheduler -> APScheduler Object
    args -> (list) list containing params dict, debug boolean \
            and msg, a string to include in response
    '''
    delay = args[2][1]
    args = [args[0],args[1],args[2][0]]
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    logger.info('Getting 10-Ks and 10-Qs Job Added')
    scheduler.add_job('Getting 10-Ks and 10-Qs %s' % args[2],getFyAndFq,args=args,trigger='date',run_date=scheduled_time)
    return 

def addGetFyAndFqList(scheduler=APScheduler,args=[]):
    '''
    Add paring 10-K and 10-Q flow to AP Scheduler.
    scheduler -> APScheduler Object
    args -> (list) list containing params dict, debug boolean \
            and msg, a string to include in response
    '''
    delay = args[2][1]
    args = [args[0],args[1],args[2][0]]
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=delay)
    logger.info('Getting 10-Ks and 10-Qs %s tickers job added' % str(len(args[2])))
    scheduler.add_job('Getting 10-Ks and 10-Qs %s tickers' % str(len(args[2])),getFyAndFqList,args=args,trigger='date',run_date=scheduled_time)
    return 

def addGetMissingTickers(scheduler=APScheduler,args=[]):
    '''
    Add missing tickers flow to AP Scheduler.
    scheduler -> APScheduler Object

    args -> (list) list containing params dict, debug boolean \
            and msg, a string to include in response
    '''
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=30)
    logger.info('Missing Tickers Job Added')
    scheduler.add_job('Missing Tickers',getMissingTickers,args=args,trigger='date',run_date=scheduled_time)
    return 

def addGetMissingFilingsIndex(scheduler=APScheduler,args=[]):
    '''
    Add missing filings flow to AP Scheduler.
    scheduler -> APScheduler Object
    args -> (list) list containing params dict, debug boolean \
            and msg, a string to include in response
    '''
    logger = logging.getLogger(__name__)
    scheduled_time = datetime.datetime.now() + datetime.timedelta(seconds=30)
    logger.info('Missing Filings Job Added')
    scheduler.add_job('Missing Filings',getMissingFilingsIndex,args=args,trigger='date',run_date=scheduled_time)
    return 

app = create_app()