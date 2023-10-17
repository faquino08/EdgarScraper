from cmath import log
from bs4 import BeautifulSoup
from numpy import append
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas.io.sql as sqlio
import os
from .xbrl_class import XBRL
from .database import databaseHandler
from ratelimiter import RateLimiter
import pandas as pd

class CompanyData:
    def __init__(self,cik=None,databaseHandler=databaseHandler,logger=None,test=True,insert=True):
        '''
        Class to get data from Edgar filings.
        cik -> (str) Central Index Key from SEC
        databaseHandler -> (databaseHandler object)
        logger -> (logging object)
        test -> (boolean) whether this is a test run
        '''
        # TWO REQUESTS ARE MADE PER FILING SO RATE IS ACTUALLY 2X THEN WHAT THE LIMITER IS SET TO. CURRENT EDGAR RATE LIMIT IS 10 PER SECOND
        rate_limiter = RateLimiter(max_calls=4,period=1,callback=self.execute_mogrify_edgarfiling)
        self.insert = insert
        self.db = databaseHandler
        self.test = test
        with rate_limiter:
            if cik is not None and databaseHandler.conn is not None:
                self.companyFilingsLoc = self.fundamentalData(cik,databaseHandler.conn)
                self.companyFilingsLoc.sort_values('FILING_DATE',ascending=False,inplace=True)
                self.logger = logger

                if len(self.companyFilingsLoc) == 0:
                    self.logger.info(str(cik)+": No filings found.")
                    return

                self.nullableInsertKeys = [
                    "EntityRegistrantName",
                    "FiscalYear",
                    "EntityCentralIndexKey",
                    "EntityFilerCategory",
                    "TradingSymbol",
                    "DocumentFiscalPeriodFocus",
                    "DocumentType",
                    "BalanceSheetDate",
                    "IncomeStatementPeriodYTD",
                    "ContextForInstants",
                    "ContextForDurations",
                    "Accession"
                ]
                self.insertKeys = [
                    "EntityRegistrantName",
                    "FiscalYear",
                    "EntityCentralIndexKey",
                    "EntityFilerCategory",
                    "TradingSymbol",
                    "DocumentFiscalYearFocus",
                    "DocumentFiscalPeriodFocus",
                    "DocumentType",
                    "BalanceSheetDate",
                    "IncomeStatementPeriodYTD",
                    "ContextForInstants",
                    "ContextForDurations",
                    "Assets",
                    "CurrentAssets",
                    "NoncurrentAssets",
                    "LiabilitiesAndEquity",
                    "Liabilities",
                    "CurrentLiabilities",
                    "NoncurrentLiabilities",
                    "CommitmentsAndContingencies",
                    "TemporaryEquity",
                    "Equity",
                    "EquityAttributableToNoncontrollingInterest",
                    "EquityAttributableToParent",
                    "Revenues",
                    "CostOfRevenue",
                    "GrossProfit",
                    "OperatingExpenses",
                    "CostsAndExpenses",
                    "OtherOperatingIncome",
                    "OperatingIncomeLoss",
                    "NonoperatingIncomeLoss",
                    "InterestAndDebtExpense",
                    "IncomeBeforeEquityMethodInvestments",
                    "IncomeFromEquityMethodInvestments",
                    "IncomeFromContinuingOperationsBeforeTax",
                    "IncomeTaxExpenseBenefit",
                    "IncomeFromContinuingOperationsAfterTax",
                    "IncomeFromDiscontinuedOperations",
                    "ExtraordaryItemsGainLoss",
                    "NetIncomeLoss",
                    "NetIncomeAvailableToCommonStockholdersBasic",
                    "PreferredStockDividendsAndOtherAdjustments",
                    "NetIncomeAttributableToNoncontrollingInterest",
                    "NetIncomeAttributableToParent",
                    "OtherComprehensiveIncome",
                    "ComprehensiveIncome",
                    "ComprehensiveIncomeAttributableToParent",
                    "ComprehensiveIncomeAttributableToNoncontrollingInterest",
                    "NonoperatingIncomeLossPlusInterestAndDebtExpense",
                    "NonoperatingIncomePlusInterestAndDebtExpensePlusIncomeFromEquityMethodInvestments",
                    "NetCashFlow",
                    "NetCashFlowsOperating",
                    "NetCashFlowsInvesting",
                    "NetCashFlowsFinancing",
                    "NetCashFlowsOperatingContinuing",
                    "NetCashFlowsInvestingContinuing",
                    "NetCashFlowsFinancingContinuing",
                    "NetCashFlowsOperatingDiscontinued",
                    "NetCashFlowsInvestingDiscontinued",
                    "NetCashFlowsFinancingDiscontinued",
                    "NetCashFlowsDiscontinued",
                    "ExchangeGainsLosses",
                    "NetCashFlowsContinuing",
                    "SGR",
                    "ROA",
                    "ROE",
                    "ROS",
                    "Accession"
                ]
                self.toInsert = []
                self.accessionNumber = {}

                # Remember tickers can be found as part of the file name in the headings of 10-K/10-Q
                i = 0
                headers = {
                    'User-Agent': 'FTC edward@ftc.com',
                }
                self.links = {}
                for index, filing in self.companyFilingsLoc.iterrows():
                    url = 'https://www.sec.gov/Archives/' + filing.HTML_lINK
                    r = requests.get(url,headers=headers)
                    res = BeautifulSoup(r.content, 'html.parser')
                    rows = res.findAll("tr")
                    self.accessionNumber[str(filing['folderName'])] = filing.HTML_lINK[filing.HTML_lINK.rindex("/")+1:filing.HTML_lINK.rindex("-")].replace('-','')
                    linkOptions = []
                    for row in rows:
                        desc = row.select("td:nth-of-type(2)")
                        if (len(desc) >= 1):
                            desc = desc[0].text.upper()
                            if (desc.find('10-K') >= 0 \
                                or desc.find('10-Q') >= 0  \
                                or desc.find('XBRL INSTANCE DOCUMENT') >= 0):
                                    linkOptions.append(row.select('a')[0].get('href'))
                    if(len(linkOptions) > 1):
                        self.links[str(filing['folderName'])] = linkOptions[-1]
                        linkOptions.clear()
                    elif(len(linkOptions) == 1):
                        self.links[str(filing['folderName'])] = linkOptions[0]
                        linkOptions.clear()
                    i += 1
                self.research()

    def research(self,downloadNonXML=False):
        '''
        Go through EDGAR filings index and parse the xml files.
        downloadNonXML -> (boolean) whether to download non xml filings
        '''
        cik = self.companyFilingsLoc.iloc[0].name
        isDir = os.path.isdir("../data/%s"%(cik))
        testIndex = 0
        headers = {
            'User-Agent': 'FTC edward@ftc.com',
        }
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        if(not isDir):
            os.makedirs("../data/%s"%(cik))
        for key in self.links:
            if(testIndex <= 0):
                self.logger.info('Accession Numbder: ' + str(self.accessionNumber[key])) 
                link = self.links[key]
                docUrl = 'https://www.sec.gov/' + link
                docRequest = session.get(docUrl,headers=headers)
                filename = link[(link.rindex('/')+1):]
                if(docUrl.find('.xml') == -1):
                    self.logger.info('Non XML File')
                    self.logger.info('Link: ' + str(docUrl))
                    if downloadNonXML:
                        dirToMake = "../data/%s/%s" %(cik,str(key))
                        isDir = os.path.isdir(dirToMake)
                        if(not isDir):
                            os.makedirs(dirToMake)
                        fileToWrite = open("../data/%s/%s/%s" %(cik,str(key),filename), "wb")
                        fileToWrite.write(docRequest.content)
                        fileToWrite.close()
                        self.logger.info('Downloading...') 
                        
                else:
                    self.logger.info('Reading XML')
                    self.logger.info('Link: ' + str(docUrl))
                    self.currentData = XBRL(xbrl_content=docRequest,tradingsymbol=False,logger=self.logger)
                    self.currentData.fields['Accession'] = self.accessionNumber[key]
                    structuredData = {}
                    for key in self.insertKeys:
                        if key in list(self.currentData.fields.keys()):
                            structuredData[key] = str(self.currentData.fields[key])
                        elif key in list(self.nullableInsertKeys):
                            structuredData[key] = "Null"
                        else:
                            structuredData[key] = 0
                    self.toInsert.append(list(structuredData.values()))
                    if self.test:
                        testIndex += 1
        if self.insert:
            self.logger.info(len(self.toInsert[-1]))
            self.logger.info(self.toInsert[-1])
            self.execute_mogrify_edgarfiling()

    def execute_mogrify_edgarfiling(self):
        '''
        Wrapper function to run execure_mogrify for the edgar filings table.
        '''
        if len(self.toInsert) > 0:
            self.db.execute_mogrify(self.toInsert,"edgarfilings")
            self.toInsert = []
            return

    def saveEmptyCik(self,cik):
        '''
        Function to save missing ciks in the postgres database.
        '''
        self.db.cur.execute('''
            CREATE TABLE 'ciksWithoutFilings' IF NOT EXISTS ("CIK" bigint,"DATEADDED" TIMESTAMP WITH TIME ZONE DEFAULT now());
        ''')
        self.db.conn.commit()
        self.db.execute_mogrify(self.db,[cik],'ciksWithoutFilings')

    def fundamentalData(self, company, conn=None):
        '''
        Get all 10-K and 10-Q index entries for a specific company.
        company -> (str) CIK of company to look up in index
        conn -> connection object from databaseHandler object
        '''
        if conn is not None:
            fundamentalIndexSql = "SELECT  \"CIK\", \"NAME\", \"FILING_TYPE\", \"FILING_DATE\", \"HTML_lINK\" FROM public.edgarindex WHERE (\"FILING_TYPE\"='10-Q' OR \"FILING_TYPE\"='10-K') AND \"CIK\"=%s ORDER BY \"FILING_DATE\";" % company
            fundamentalLinks = sqlio.read_sql_query(fundamentalIndexSql,conn,index_col="CIK")
            fundamentalLinks['folderName'] = ''
            for index,row in fundamentalLinks.iterrows():
                row.folderName = row.HTML_lINK[(row.HTML_lINK.rindex('/')+12):-11]
            return fundamentalLinks
        else:
            raise Exception("No Postgres Connection Provided")