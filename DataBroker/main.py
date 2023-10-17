from asyncio.log import logger
import logging
import datetime
import time
import psycopg2
import psycopg2.extras
import pandas as pd
import pandas.io.sql as sqlio
from DataBroker.Sources.Edgar.secFunctions import secFunctions
from types import SimpleNamespace
from sqlalchemy import create_engine

class Main:
    def __init__(self,postgresParams={},debug=False):
        '''
        Main class for running different funcions for SEC EDGAR.
        postgresParams -> (dict) Dict with keys host, port, database, user, \
                            password for Postgres database
        debug -> (boolean) Whether to record debug logs
        '''
        self.today = datetime.date.today()
        self.data = {}
        if debug:
            logging.basicConfig(
                level=logging.DEBUG,
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                datefmt="%m-%d %H:%M:%S",
            )
        else:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                datefmt="%m-%d %H:%M:%S",
            )
        
        self.log = logging.getLogger(__name__)
        self.params = postgresParams
        self.startTime = time.time() 
        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'Main')
        self.log.info(f'Starting Run at: {self.startTime}')
        self.edgar = secFunctions(postgresParams,debug)

    def edgar_getFyAndFq(self,cik):
        '''
        Function for getting 10-K and 10-Q data for a company.
        cik -> (str) Central Index Key from SEC
        '''
        self.edgar.getFyAndFqReports(cik,False,True)
        return

    def edgar_getMissingTickers(self,date):
        '''
        Function to get tickers from 10-K and 10-Q filings that aren't in our database.
        date -> (str) "YYYY-MM-dd" Earliest date of filings to parse in \
                edgarindex
        '''
        self.edgar.getTickersNotInDb(date=date)
        return

    def edgar_getMissingFilingsIndex(self,year):
        '''
        Function to get filing entries missing from our database index.
        year -> (int) cut off year to look for missing filing entries
        '''
        self.edgar.getIndexEntriesOfFilingsMissing(year)
        return

    def exit(self):
        '''
        Exit class. Log Runtime. And shutdown logging.
        '''
        self.edgar.exit()
        self.log.info("Closing Main")
        self.log.info(f'Ending Run at: {self.endTime}')
        self.log.info(f'Runtime: {self.endTime - self.startTime}')
        logging.shutdown()