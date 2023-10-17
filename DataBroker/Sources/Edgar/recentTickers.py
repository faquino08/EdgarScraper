import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas.io.sql as sqlio
from .xbrl_class import XBRL
from .database import databaseHandler
from ratelimiter import RateLimiter
import os

class recentTickers:
    def __init__(self,date='2021-12-01',databaseHandler=databaseHandler,downloadHTML=False,logger=None):
        '''
        Class to get ticker symbols from EDGAR 10-K and 10-Q filings going back to given date.
        date -> (str) Date 'YYY-mm-dd'
        databaseHandler -> (Object) Database handler
        downloadHTML -> (boolean) whether to download non XML filings
        logger -> (Object) logging object
        '''
        # THREE REQUESTS ARE MADE PER FILING SO RATE IS ACTUALLY 3X THEN WHAT THE LIMITER IS SET TO. CURRENT EDGAR RATE LIMIT IS 10 PER SECOND
        self.rate_limiter = RateLimiter(max_calls=2,period=1,callback=self.execute_mogrify_tickers)
        if databaseHandler.conn is not None:
            mostRecentCiksSql = "SELECT  ei.\"CIK\", ei.\"NAME\", ei.\"FILING_TYPE\", ei.\"FILING_DATE\", ei.\"TXT_LINK\", ei.\"HTML_lINK\" \
                FROM (SELECT \"CIK\", \"FILING_TYPE\", \"FILING_DATE\" AS \
                    \"MOST_RECENT_FILING\" FROM PUBLIC.EDGARINDEX WHERE \
                    \"FILING_DATE\" > '%s' AND (\"FILING_TYPE\"='10-Q' OR \
                    \"FILING_TYPE\"='10-K') GROUP BY \"CIK\",\"FILING_TYPE\", \
                    \"FILING_DATE\") r INNER JOIN PUBLIC.EDGARINDEX ei \
                    ON ei.\"CIK\" = r.\"CIK\" AND ei.\"FILING_DATE\" = r.\
                    \"MOST_RECENT_FILING\" AND ei.\"FILING_TYPE\" = r.\"FILING_TYPE\" WHERE ei.\"CIK\" NOT IN (SELECT \"CIK\" FROM PUBLIC.EDGARTICKERINDEX) ORDER BY \"MOST_RECENT_FILING\";" % date
            self.logger = logger
            self.date = date
            self.downloadHTML = downloadHTML
            self.headers = {
                'User-Agent': 'FTC edward@ftc.com',
            }
            self.mostRecentCiksLinks = sqlio.read_sql_query(mostRecentCiksSql,databaseHandler.conn,index_col="CIK")
            self.alreadyHaveCiks = []
            self.inDatabaseCiksSql = "SELECT \"CIK\" FROM PUBLIC.EDGARTICKERINDEX"
            self.inDatabaseCiks = sqlio.read_sql_query(self.inDatabaseCiksSql,databaseHandler.conn,index_col="CIK")
            logger.info("In Database Ciks:")
            logger.info(self.inDatabaseCiks)
            logger.info("In Database Ciks Indices:")
            logger.info(self.inDatabaseCiks.index.values)
            self.alreadyHaveCiks += list(self.inDatabaseCiks.index.values)
            self.databaseHandler = databaseHandler
            self.mostRecentTickers = []
            self.run()
        else:
            raise Exception("No Postgres Connection Provided")
            return
    
    def run(self):
        '''
        Function to run workflow to get ticker symbols from EDGAR 10-K and 10-Q filings going back to given date.
        '''
        for index,filing in self.mostRecentCiksLinks.iterrows():
            cik = index
            url = 'https://www.sec.gov/Archives/' + filing.HTML_lINK
            r = self.fetchSecLink(url,self.headers)
            res = BeautifulSoup(r.content, 'html.parser')

            rows = res.findAll("tr")
            
            linkOptions = {}
            for row in rows:
                desc = row.select("td:nth-of-type(2)") 
                typeFiling = row.select("td:nth-of-type(4)")
                if (len(desc) >= 1):
                    desc = desc[0].text.upper()
                    if (desc.find('10-K') >= 0):
                            start = desc.find('10-K')
                            linkOptions[str(desc[start:start+4])] = row.select('a')[0].get('href')
                    elif desc.find('10-Q') >= 0:
                        start = desc.find('10-Q')
                        linkOptions[str(desc[start:start+4])] = row.select('a')[0].get('href')
                    elif desc.find('XBRL INSTANCE DOCUMENT') >= 0:
                        start = desc.find('XBRL INSTANCE DOCUMENT')
                        linkOptions[str(desc[start:start+4])] = row.select('a')[0].get('href')
                if (len(typeFiling) >= 1):
                    typeFiling = typeFiling[0].text.upper()
                    if (typeFiling.find('10-K') >= 0 \
                        or typeFiling.find('10-Q') >= 0  \
                        or typeFiling.find('XML') >= 0) :
                            linkOptions[typeFiling] = row.select('a')[0].get('href')
            if(len(linkOptions) > 1):
                if "XML" in linkOptions:
                    link = linkOptions["XML"]
                    linkOptions.clear()
                elif "XBRL" in linkOptions:
                    link = linkOptions["XBRL"]
                    linkOptions.clear()
                elif "10-K" in linkOptions:
                    link = linkOptions["10-K"]
                    linkOptions.clear()
                elif "10-Q" in linkOptions:
                    link = linkOptions["10-Q"]
                    linkOptions.clear()
                else:
                    self.logger.error("Error: Can't recognize key: " + str(linkOptions))
            elif(len(linkOptions) == 1):
                linkKey = list(linkOptions.keys())
                link = linkOptions[linkKey[0]]
                linkOptions.clear()

            docUrl = 'https://www.sec.gov/' + link

            if(docUrl.find('.xml') == -1):
                # Write Files to Local Folder if not xml
                if self.downloadHTML:
                    docRequest = self.fetchSecLink(docUrl,self.headers)
                    filename = link[(link.rindex('/')+1):]
                    firstDirToMake = "./recentTickers"
                    isFirstDir = os.path.isdir(firstDirToMake)
                    if (not isFirstDir):
                        os.mkdir(firstDirToMake)
                    dirToMake = firstDirToMake + "/%s" %(cik)
                    isDir = os.path.isdir(dirToMake)
                    if(not isDir):
                        os.mkdir(dirToMake)
                        fileToWrite = open("./recentTickers/%s/%s" %(cik,filename), "wb")
                        fileToWrite.write(docRequest.content)
                        fileToWrite.close()
                    else:
                        fileToWrite = open("./recentTickers/%s/%s" %(cik,filename), "wb")
                        fileToWrite.write(docRequest.content)
                        fileToWrite.close()
            else:
                docRequest = self.fetchSecLink(docUrl,headers=self.headers)
                data = XBRL(xbrl_content=docRequest, tradingsymbol=True,logger=self.logger)
                if data.fields['TradingSymbol'] != 'N/A' and data.fields['TradingSymbol'] != 'Not Provided' and ['TradingSymbol'] != 'NON-XML':
                    if cik not in self.alreadyHaveCiks:
                        self.mostRecentTickers.append([cik,data.fields['TradingSymbol'],docUrl])
                        self.alreadyHaveCiks.append(cik)
                else:
                    self.logger.info(f'')
                    self.logger.info(f'CIK: {cik}')
                    self.logger.info(f'Symbol: {data.fields["TradingSymbol"]}')
                    self.logger.info(f'')
        self.databaseHandler.execute_mogrify(index=self.mostRecentTickers,\
            table="edgartickerindex")#,date=self.date)
        return self.mostRecentTickers

    def fetchSecLink(self,link,headers):
        '''
        Function to make requests to EDGAR API with rate_limiter.
        link -> (str) address to filing to request
        header -> (dict) headers to send in API requests
        '''
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        with self.rate_limiter:
            return session.get(link,headers=headers)

    def execute_mogrify_tickers(self,until):
        '''
        Wrapper function for databaseHandler.execute_mogrify to run as callback for rate_limiter.
        '''
        self.databaseHandler.execute_mogrify(index=self.mostRecentTickers,table="edgartickerindex")#,date=self.date)
        self.mostRecentTickers.clear()
        return