import sys
import psycopg2
import psycopg2.extras
import os
from os.path import exists
import csv
import logging
import datetime
import time

logger = logging.getLogger(__name__)
class databaseHandler:
    def __init__(self,params_dic={}):
        '''
        Database wrapper for common SQL queries and handling database connection.
        params_dic -> Dict with keys host, port, database, user, \
                            password for Postgres database
        logger -> logging object
        '''
        self.params = params_dic
        self.logger = logger
        self.conn = None
        self.cur = None
        self.batch_size = 1000000
        self.connect()

    def connect(self):
        '''
        Connect to the PostgreSQL database server
        '''
        try:
            # connect to the PostgreSQL server
            self.logger.debug('Connecting to the PostgreSQL database...')
            self.conn = psycopg2.connect(**self.params)
            self.cur = self.conn.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error(error)
            sys.exit(1)
        self.logger.debug("Connection successful")
        return

    def composeSqlColumnsPlaceholders(self,dataSample=[]):
        '''
        Takes list and create string of placeholders for each entry in list for execute_mogrify.
        dataSample -> (list) List of columns to insert
        '''
        result = "("
        i = 1
        while i <= len(dataSample):
            result += "%s,"
            i += 1
        result = result[:-1] + ")"
        return result
    
    def execute_mogrify(self,index,table=None):
        '''
        Takes dataframe and inserts the data into the provided table using execute_mogrify.
        index -> (Dataframe) data to insert into database
        table -> (str) Name of Postgres Table
        '''
        if table is not None:
            if table == "edgarindex":
                if (len(index) > 0):
                    str_placholders = self.\
                        composeSqlColumnsPlaceholders(dataSample=index[0])
                    for i in range(0, len(index), self.batch_size):
                        args_str = ','.join(self.cur.mogrify(str_placholders,row).decode('utf-8') for row in index[i:i+self.batch_size])
                        self.logger.info(f"Inserting master index from EDGAR")
                        try:
                            self.cur.execute('INSERT INTO "edgarindex" VALUES' + args_str)
                            self.conn.commit()
                        except (Exception, psycopg2.DatabaseError) as error:
                            self.logger.error("Error: %s" % error)
                            self.conn.rollback()
            if table == "edgartickerindex":
                #index['date'] = datetime.date.today().strftime('%Y-%m-%d')
                if (len(index) > 0):
                    str_placholders = self.\
                        composeSqlColumnsPlaceholders(dataSample=index[0])
                    self.logger.info("Inserting %s tickers from recent SEC Filings into  edgartickerindex" % (len(index)))
                    index
                    for i in range(0, len(index), self.batch_size):
                        args_str = ','.join(self.cur.mogrify(str_placholders,row).decode('utf-8') for row in index[i:i+self.batch_size])
                        try:
                            self.cur.execute('INSERT INTO "edgartickerindex" VALUES' + args_str)
                            self.conn.commit()
                        except (Exception, psycopg2.DatabaseError) as error:
                            self.logger.error("Error: %s" % error)
                            self.conn.rollback()
            if table == "edgarfilings":
                if (len(index) > 0):
                    str_placholders = self.\
                        composeSqlColumnsPlaceholders(dataSample=index[0])
                    for i in range(0, len(index)+1, self.batch_size):
                        if ((i+self.batch_size) < len(index)):
                            batch_max = i+self.batch_size
                        elif (i+self.batch_size >= len(index)):
                            batch_max = len(index)-1
                        if len(index) == 1:
                            batch_max = 1
                        tuples = [tuple(x) for x in index[i:batch_max]]
                        values = [self.cur.mogrify(str_placholders, tup).decode('utf8') for tup in tuples]
                        args_str = ",".join(values)
                        args_str = args_str.replace("\'NULL\'","NULL")
                        self.logger.info("Inserting filing data from Edgar...")
                        self.logger.debug('INSERT INTO public.edgarfilings VALUES %s ON CONFLICT ON CONSTRAINT accession DO NOTHING;' % args_str)
                        try:
                            self.cur.execute('INSERT INTO public.edgarfilings VALUES %s ON CONFLICT ON CONSTRAINT accession DO NOTHING;' % args_str)
                            self.conn.commit()
                        except (Exception, psycopg2.DatabaseError) as error:
                            self.logger.error("Error: %s" % error)
                            self.conn.rollback()
            else:
                if (len(index) > 0):
                    str_placholders = self.\
                        composeSqlColumnsPlaceholders(dataSample=index[0])
                    for i in range(0, len(index)+1, self.batch_size):
                        if ((i+self.batch_size) < len(index)):
                            batch_max = i+self.batch_size
                        elif (i+self.batch_size >= len(index)):
                            batch_max = len(index)-1
                        if len(index) == 1:
                            batch_max = 1
                        tuples = [tuple(x) for x in index[i:batch_max]]
                        values = [self.cur.mogrify(str_placholders, tup).decode('utf8') for tup in tuples]
                        args_str = ",".join(values)
                        args_str = args_str.replace("\'NULL\'","NULL")
                        self.logger.info("Inserting filing data from Edgar...")
                        self.logger.debug('INSERT INTO public.\'%s\' VALUES %s ON CONFLICT DO NOTHING;' % (table,args_str))
        else:
            raise Exception("Need to provide table for execute_mogrify.")

    def getLastDate(self,table,column):
        '''
        Get largest date in Postgres table.
        table -> (str) Name of Postgres table
        column -> (str) Name of Postgres date column
        '''
        lastTimeSql = "SELECT MAX(\"{}\") FROM {}"
        sqlComm = lastTimeSql.format(column,table)
        try:
            self.cur.execute(sqlComm)
            lastDate = self.cur.fetchone()[0]
            return lastDate
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("Error: %s" % error)
            self.conn.rollback()
            return None
        
    def getFirstDate(self,table,column):
        '''
        Get largest date in Postgres table.
        table -> (str) Name of Postgres table
        column -> (str) Name of Postgres date column
        '''
        lastTimeSql = "SELECT MIN(\"{}\") FROM {}"
        sqlComm = lastTimeSql.format(column,table)
        try:
            self.cur.execute(sqlComm)
            lastDate = self.cur.fetchone()[0]
            return lastDate
        except (Exception, psycopg2.DatabaseError) as error:
            self.logger.error("Error: %s" % error)
            self.conn.rollback()
            return None

    def getNewIndexEntries(self):
        '''
        Go through master index file and search for entries not in our database.
        '''
        file_exists = exists("./index/master.csv")
        if (file_exists):
            with open("./index/master.csv") as file:
                # Passing the TSV file to 
                # reader() function
                # with tab delimiter
                # This function will
                # read data from file
                tsv_file = csv.reader(file, delimiter="|")

                # Getting today's date
                todays_Date = datetime.date.fromtimestamp(time.time())
                i = 0
                #for row in tsv_file: i += 1
                lastEntry = self.getLastDate("edgarindex","FILING_DATE")
                firstEntry = self.getFirstDate("edgarindex","FILING_DATE")
                self.logger.info('To Add: ' + str(i))
                self.logger.info('First Entry: ' + str(firstEntry))
                self.logger.info('Last Entry: ' + str(lastEntry))
                self.logger.info('Today\'s Date: ' + str(todays_Date))
                self.logger.info(firstEntry)
                self.logger.info(lastEntry)
                filingsToCheckDate = [row for row in tsv_file]
                dates = [row[3] for row in filingsToCheckDate]
                self.logger.info('Length of filingsToCheckDate:')
                self.logger.info(str(len(filingsToCheckDate)))
                self.tsv_results = []
                if lastEntry is not None:
                    self.tsv_results = [row for row in filingsToCheckDate if (datetime.date.fromisoformat(row[3]) > lastEntry and datetime.date.fromisoformat(row[3])+datetime.timedelta(days=2)  <= todays_Date) or (datetime.date.fromisoformat(row[3]) < firstEntry)]
                    '''for row in filingsToCheckDate:
                        rowDate = datetime.date.fromisoformat(row[3])
                        if  ( rowDate > lastEntry ):
                                #and \
                            #rowDate+datetime.timedelta(days=2)  <= todays_Date):#\
                                #or \
                            #(datetime.date.fromisoformat(row[3]) < firstEntry):
                                self.tsv_results.append(row)'''
                    '''self.logger.info('Length of first test:')
                    self.logger.info(str(len(
                        [row for row in tsv_file if (datetime.date.fromisoformat(row[3]) > lastEntry and datetime.date.fromisoformat(row[3])+datetime.timedelta(days=2)  <= todays_Date)])))
                    self.logger.info('Length of second test:')
                    self.logger.info(str(len(
                        [row for row in tsv_file if (datetime.date.fromisoformat(row[3]) < firstEntry)])))'''
                else:
                    self.tsv_results = filingsToCheckDate
            self.logger.info('Length of tsv_results:')
            self.logger.info(str(len(self.tsv_results)))
            self.logger.info(str(dates[-10:]))
            self.execute_mogrify(self.tsv_results,"edgarindex")
            #Linux
            os.system('rm -rf ./index/master.csv')
        else:
            self.logger.info("")
            self.logger.info("No Master.csv to insert")
        return

    def exit(self):
        '''
        Exit class and close Postgres connection
        '''
        self.cur.close()
        self.conn.close()
        self.logger.info('Db Exit Status:')
        self.logger.info('Psycopg2:')
        self.logger.info(self.conn.closed)