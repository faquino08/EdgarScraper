import logging
import time
import datetime
import inspect
from .get_edgar_index import get_edgar_index
from .database import databaseHandler
from .research import CompanyData
from .recentTickers import recentTickers

class secFunctions:
    def __init__(self,postgresParams={},debug=False):
        '''
        Class to perform functions on EDGAR API.
        postgresParams -> (dict) Dict with keys host, port, database, user, \
                            password for Postgres database
        debug -> (boolean) Whether to record debug logs
        '''
        if debug:
            logging.basicConfig(
                level=logging.DEBUG,
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                datefmt="%m-%d %H:%M:%S"
            )
        else:
            logging.basicConfig(
                level=logging.INFO,
                format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
                datefmt="%m-%d %H:%M:%S"
            )
        self.log = logging.getLogger(__name__)
        self.postgres = postgresParams
        # Connect to Postgres
        self.db = databaseHandler(self.postgres)
        self.startTime = time.time()
        self.caller = inspect.stack()[1][3].upper()

        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'')
        self.log.info(f'Edgar Databroker')
        self.log.info(f'Starting Run at: {self.startTime}')

    def getFyAndFqReports(self,cik=None,test=False,insert=True):
        '''
        Flow to get 10-K and 10-Q filings for a company.
        cik -> (str) Central Index Key from SEC
        test -> (boolean) whether this is a test run
        insert -> (boolean) whether to insert into database
        '''
        if cik is not None:
            startTime = time.time()

            self.caller = inspect.stack()[0][3].upper()

            # Create New Run in RunHistory
            self.db.cur.execute('''
                INSERT INTO PUBLIC.financedb_RUNHISTORY ("Process","Startime","SymbolsToFetch") VALUES ('%s','%s',0) RETURNING "Id";
            ''' % (self.caller,startTime))
            self.runId = self.db.cur.fetchone()[0]

            self.log.info('')
            self.log.info(f'Fetching Annual and Fiscal Reports for: {cik}')
            self.log.info(f'Cik: {cik}')
            self.log.info(f'Start: {startTime}')
            CompanyData(cik,self.db,self.log,test,insert)
            self.exit()
            return
        
    def getFyAndFqReportsList(self,ciks=None,test=False,insert=True):
        '''
        Flow to get 10-K and 10-Q filings for a list of companies.
        cik -> (str) Central Index Key from SEC
        test -> (boolean) whether this is a test run
        insert -> (boolean) whether to insert into database
        '''
        if ciks is not None:
            startTime = time.time()

            self.caller = inspect.stack()[0][3].upper()

            # Create New Run in RunHistory
            self.db.cur.execute('''
                INSERT INTO PUBLIC.financedb_RUNHISTORY ("Process","Startime","SymbolsToFetch") VALUES ('%s','%s',0) RETURNING "Id";
            ''' % (self.caller,startTime))
            self.runId = self.db.cur.fetchone()[0]            

            for cik in ciks:
                self.log.info('')
                self.log.info(f'Fetching Annual and Fiscal Reports for: {cik}')
                self.log.info(f'Cik: {cik}')
                self.log.info(f'Start: {startTime}')
                CompanyData(cik,self.db,self.log,test,insert)
                self.exit()
            return

    def getTickersNotInDb(self,date="2022-01-01"):
        '''
        Parse all 10-K and 10-Q filings going back to a certain date for their tickers and insert into database if not already there.
        date -> (str) "YYYY-MM-dd" Earliest date of filings to parse in \
                edgarindex
        '''
        startTime = time.time()

        self.caller = inspect.stack()[0][3].upper()

        # Create New Run in RunHistory
        self.db.cur.execute('''
            INSERT INTO PUBLIC.financedb_RUNHISTORY ("Process","Startime","SymbolsToFetch") VALUES ('%s','%s',0) RETURNING "Id";
        ''' % (self.caller,startTime))
        self.runId = self.db.cur.fetchone()[0]
        
        self.log.info('')
        self.log.info(f'Getting Tickers Not in DB from:')
        self.log.info(f'Date: {date}')
        self.log.info(f'Start: {startTime}')
        recentTickers(date=date,databaseHandler=self.db,downloadHTML=False,logger=self.log)
        self.exit()
        return

    def getIndexEntriesOfFilingsMissing(self,year=2022,endYear=None):
        '''
        Get entries to the SEC Edgar Filings Index that are missing from the edgarindex table in our databse.
        year -> (int) cut off year to look for missing filing entries
        '''
        startTime = time.time()

        self.caller = inspect.stack()[0][3].upper()

        self.log.info('')
        self.log.info(f'Getting Index Entries of SEC Filings Missing from DB')
        self.log.info(f'Year: {year}')
        self.log.info(f'Start: {startTime}')
        if endYear is None: endYear = datetime.datetime.today().year
        get_edgar_index(year,endYear)
        self.db.getNewIndexEntries()
        self.exit()
        return
        
    def exit(self):
        '''
        Function to exit class.
        '''
        self.endTime = time.time()

        # Update RunHistory With EndTime
        self.db.cur.execute('''
            UPDATE PUBLIC.financedb_RUNHISTORY
            SET "Endtime"=%s,
                "SymbolsInsert"=0,
            WHERE "Id"=%s
        ''' % (self.endTime,self.runId))

        self.db.exit()
        self.log.info(f'Ending Run at: {self.endTime}')
        self.log.info(f'Duration: {self.endTime-self.startTime}')
        self.log.info('')
        self.log.removeHandler(self.fh)