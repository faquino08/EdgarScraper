from bs4 import BeautifulSoup
import re
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import unicodedata

headers = {
    'User-Agent': 'FTC edward@ftc.com',
}

def has_segment_and_explicitmember(tag):
        for child in tag.children:
            x = re.compile('segment')
            if x.match(str(child.name)):
                explicitMembers = len(child.find_all(re.compile('explicitMember')))
                if explicitMembers > 0:
                    return True
                else:
                    return False
            else:
                return False

def has_nil_attr(tag):
    if len(tag.attrs) > 0:
        for key in tag.attrs.keys():
            if key.endswith('nil'):
                if tag[key] == "true":
                    return True

class XBRL:
    def __init__(self,xbrlurl=None,xbrl_content=None,tradingsymbol=True,\
        logger=None):
        '''
        Initiate XBRL class object to parse filings.
        xbrlurl -> (string) url to xbrl filing to parse
        xbrl_content -> (GET request) get request from requests library
        tradingsymbol -> (boolean) only get basic info such as ticker
        '''
        self.fields = {}
        self.logger = logger
        session = requests.Session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        if xbrlurl is not None:
            self.xbrlurl = xbrlurl
            xbrl_res = session.get(xbrlurl,headers=headers)
            xbrl_str = xbrl_res.content
            self.parsedXbrl = BeautifulSoup(xbrl_str,'lxml-xml')
        elif xbrl_content is not None:
            xbrl_str = xbrl_content.content
            self.xbrlurl = xbrl_content.url
            self.parsedXbrl = BeautifulSoup(xbrl_str,'lxml-xml')
        if tradingsymbol:
            self.GetBaseInformation()
        else:
            self.GetBaseInformation()
            self.getData()
        
    def getData(self):
        '''
        Gets the ending date of this filing period to be able to get the current context ref of the filing and finally pull data.
        '''
        currentEnd = self.parsedXbrl.find('dei:DocumentPeriodEndDate').text
        asdate = re.match('\s*(\d{4})-(\d{2})-(\d{2})\s*', currentEnd)
        if asdate:
            thisend = '%s-%s-%s' % (asdate.groups()[0],asdate.groups()[1],asdate.groups()[2])                    
            self.GetCurrentPeriodAndContextInformation(thisend)
            self.fundamentalAccountingConcepts()
            return
        else:
            self.logger.debug(currentEnd + ' is not a date')
            return

    def GetFactValue(self,SeekConcept, ConceptPeriodType):
        '''
        Function for easily getting specific field from xbrl filing.
        SeekConcept -> (str) Field to search document
        ConceptPeriodType -> ("Instant" or "Duration") Which context to use based on type of field
        '''
        factValue = None
            
        if ConceptPeriodType == "Instant":
            ContextReference = self.fields['ContextForInstants']
        elif ConceptPeriodType == "Duration":
            ContextReference = self.fields['ContextForDurations']
        else:
            #An error occured
            return "CONTEXT ERROR"
        
        if not ContextReference:
            return None


        oNode = self.parsedXbrl.find(str(SeekConcept),attrs={'contextRef':str(ContextReference)})
        if oNode is not None:                    
            factValue = oNode.text
            if has_nil_attr(oNode):
                factValue=0
            try:
                factValue = float(factValue)
            except:
                self.logger.error('couldnt convert %s=%s to string' % (SeekConcept,factValue))
                factValue = None
                pass
            
        return factValue   

    def GetBaseInformation(self):
        '''
        Get basic information that doesn't depend on context ref. In other words information that doesnn't change frequently like entity name.
        '''               
        #Registered Name
        oNode = self.parsedXbrl.find('dei:EntityRegistrantName')
        if oNode is not None:
            self.fields['EntityRegistrantName'] = oNode.text
            unicodedata.normalize('NFD', self.fields['EntityRegistrantName']).encode('ascii', 'ignore')
        else:
            self.fields['EntityRegistrantName'] = "NULL"

        #Fiscal year
        oNode = self.parsedXbrl.find('dei:CurrentFiscalYearEndDate')     
        if oNode is not None:
            self.fields['FiscalYear'] = oNode.text
            unicodedata.normalize('NFD', self.fields['FiscalYear']).encode('ascii', 'ignore')
        else:
            self.fields['FiscalYear'] = "NULL"

        #EntityCentralIndexKey
        oNode = self.parsedXbrl.find("dei:EntityCentralIndexKey")
        if oNode is not None:
            self.fields['EntityCentralIndexKey'] = oNode.text
            unicodedata.normalize('NFD', self.fields['EntityCentralIndexKey']).encode('ascii', 'ignore')
        else:
            self.fields['EntityCentralIndexKey'] = "NULL"

        #EntityFilerCategory
        oNode = self.parsedXbrl.find("dei:EntityFilerCategory")
        if oNode is not None:
            self.fields['EntityFilerCategory'] = oNode.text
            unicodedata.normalize('NFD', self.fields['EntityFilerCategory']).encode('ascii', 'ignore')
        else:
            self.fields['EntityFilerCategory'] = "NULL"

        #TradingSymbol
        oNode = self.parsedXbrl.find("dei:TradingSymbol")
        if oNode is not None:
            self.fields['TradingSymbol'] = oNode.text
            unicodedata.normalize('NFD', self.fields['TradingSymbol']).encode('ascii', 'ignore')
        else:
            self.fields['TradingSymbol'] = "Not Provided"

        #DocumentFiscalYearFocus
        oNode = self.parsedXbrl.find("dei:DocumentFiscalYearFocus")
        if oNode is not None:
            self.fields['DocumentFiscalYearFocus'] = oNode.text
            unicodedata.normalize('NFD', self.fields['DocumentFiscalYearFocus']).encode('ascii', 'ignore')
        else:
            self.fields['DocumentFiscalYearFocus'] = "NULL"

        #DocumentFiscalPeriodFocus
        oNode = self.parsedXbrl.find("dei:DocumentFiscalPeriodFocus")
        if oNode is not None:
            self.fields['DocumentFiscalPeriodFocus'] = oNode.text
            unicodedata.normalize('NFD', self.fields['DocumentFiscalPeriodFocus']).encode('ascii', 'ignore')
        else:
            self.fields['DocumentFiscalPeriodFocus'] = "NULL"
        
        #DocumentType
        oNode = self.parsedXbrl.find("dei:DocumentType")
        if oNode is not None:
            self.fields['DocumentType'] = oNode.text
            unicodedata.normalize('NFD', self.fields['DocumentType']).encode('ascii', 'ignore')
        else:
            self.fields['DocumentType'] = "NULL"

        self.logger.info(" ")
        self.logger.debug("FUNDAMENTAL ACCOUNTING CONCEPTS CHECK REPORT:")
        self.logger.debug("XBRL instance: %s" % self.xbrlurl)
        self.logger.debug("XBRL Cloud Viewer: https://edgardashboard.xbrlcloud.com/flex/viewer/XBRLViewer.html#instance=%s" % self.xbrlurl)
        
        self.logger.info("Entity regiant name: %s" % self.fields['EntityRegistrantName'])
        self.logger.debug("CIK: %s" % self.fields['EntityCentralIndexKey'])
        self.logger.debug("Entity filer category: %s" % self.fields['EntityFilerCategory'])
        self.logger.debug("Trading symbol: %s" % self.fields['TradingSymbol'])
        self.logger.debug("Fiscal year: %s" % self.fields['DocumentFiscalYearFocus'])
        self.logger.debug("Fiscal period: %s" % self.fields['DocumentFiscalPeriodFocus'])
        self.logger.debug("Document type: %s" % self.fields['DocumentType'])
        self.logger.info(" ")

    def GetCurrentPeriodAndContextInformation(self, EndDate):
        '''
        Get the context ref for the most recent filing.
        EndDate -> (str) "YYYY-MM-dd" Last date of filing period
        '''
        #Figures out the current period and contexts for the current period instance/duration contexts
        self.fields['BalanceSheetDate'] = "ERROR"
        self.fields['IncomeStatementPeriodYTD'] = "ERROR"
        
        self.fields['ContextForInstants'] = "ERROR"
        self.fields['ContextForDurations'] = "ERROR"

        #This finds the period end date for the database table, and instant date (for balance sheet):        
        UseContext = "ERROR"

        #This is the <instant> or the <endDate>
        oNodelist2 = self.parsedXbrl.find_all(['us-gaap:Assets', 'us-gaap:AssetsCurrent', 'us-gaap:LiabilitiesAndStockholdersEquity'])

        #Nodelist of all the facts which are us-gaap:Assets
        for i in oNodelist2:            
            ContextID = i['contextRef']
            #Nodelist of all the contexts of the fact us-gaap:Assets
            oNodelist3 = self.parsedXbrl.find_all('context',attrs={'id': ContextID})
            if len(oNodelist3) == 0:
                oNodelist4 = self.parsedXbrl.find_all('xbrli:context',attrs={'id': ContextID})
            if len(oNodelist3) == 0:
                    if len(oNodelist4) > 0:
                        self.logger.debug("xbrli:period: " + str(oNodelist4[0]))

            for j in oNodelist3:
                #Nodes with the right period
                if j.period.instant is not None and j.period.instant.text==EndDate:
                    
                    oNode4 = j.find_all(has_segment_and_explicitmember)
                    if not len(oNode4):
                        UseContext = ContextID
        
        ContextForInstants = UseContext
        self.fields['ContextForInstants'] = ContextForInstants

        
        ###This finds the duration context
        ###This may work incorrectly for fiscal year ends because the dates cross calendar years
        #Get context ID of durations and the start date for the database table
        oNodelist2 = self.parsedXbrl.find_all(['us-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease','us-gaap:CashPeriodIncreaseDecrease', 'us-gaap:NetIncomeLoss', 'dei:DocumentPeriodEndDate'])

        StartDate = "ERROR"
        StartDateYTD = "2099-01-01"
        UseContext = "ERROR"

        for i in oNodelist2:
            
            ContextID = i['contextRef']
            
            #Nodelist of all the contexts of the fact us-gaap:Assets
            oNodelist3 = self.parsedXbrl.find_all('context',attrs={'id': ContextID})
            if len(oNodelist3) == 0:
                oNodelist4 = self.parsedXbrl.find_all('xbrli:context',attrs={'id': ContextID})
            if len(oNodelist3) == 0:
                    if len(oNodelist4) > 0:
                        self.logger.debug("xbrli:period: " + str(oNodelist4[0]))

            for j in oNodelist3:
                #Nodes with the right period
                if j.period.endDate.text==EndDate:
                    
                    oNode4 = j.find_all(has_segment_and_explicitmember)
                    if not len(oNode4): 
                    
                        #Get the year-to-date context, not the current period
                        StartDate = j.period.startDate.text
                        self.logger.debug("Context start date: " + StartDate)
                        self.logger.debug("YTD start date: " + StartDateYTD)
                        
                        if StartDate <= StartDateYTD:
                            #MsgBox "YTD is greater"
                            #Start date is for quarter
                            self.logger.debug("Context start date is less than current year to date, replace")
                            self.logger.debug("Context start date: " + StartDate)
                            self.logger.debug("Current min: " + StartDateYTD)
                            
                            StartDateYTD = StartDate
                            UseContext = j['id']
                        else:
                            #MsgBox "Context is greater"
                            #Start date is for year
                            self.logger.debug("Context start date is greater than YTD, keep current YTD")
                            self.logger.debug("Context start date: " + StartDate)
                            
                            StartDateYTD = StartDateYTD

                        
                        self.logger.debug("Use context ID: " + UseContext)
                        self.logger.debug("Current min: " + StartDateYTD)
                        self.logger.debug(" ")
                                        
                        self.logger.debug("Use context: " + UseContext)
                            

        #Balance sheet date of current period
        self.fields['BalanceSheetDate'] = EndDate
        
        #MsgBox "Instant context is: " + ContextForInstants
        if self.fields['ContextForInstants']=="ERROR":
            #MsgBox "Looking for alternative instance context"
            
            ContextForInstants = self.LookForAlternativeInstanceContext()
            self.fields['ContextForInstants'] = ContextForInstants
        
        
        #Income statement date for current fiscal year, year to date
        self.fields['IncomeStatementPeriodYTD'] = StartDateYTD
        
        ContextForDurations = UseContext
        self.fields['ContextForDurations'] = ContextForDurations

    def LookForAlternativeInstanceContext(self):
        '''
        In case a context ref for instants period types couldn't be found, this uses an alternative method for finding the context ref.
        '''
        #This deals with the situation where instance context has no dimensions            
        something = None
        
        #See if there are any nodes with the document period focus date
        oNodeList_Alt = self.parsedXbrl.find_all('instant',string=self.fields['BalanceSheetDate'])
        self.logger.debug("Possible Contexts: " + str(len(oNodeList_Alt)))

        #MsgBox "Node list length: " + oNodeList_Alt.length
        for oNode_Alt in oNodeList_Alt:
            if oNode_Alt.parent.parent.name == 'context':
                context = oNode_Alt.parent.parent
            self.logger.debug("Possible ID: " + context['id'])
            
            #Found possible contexts
            #MsgBox context.selectSingleNode("@id").text
            something = self.parsedXbrl.find('us-gaap:Assets',attrs={'contextRef': context["id"]})

            if something:
                #MsgBox "Use this context: " + context.selectSingleNode("@id").text
                self.logger.debug("Alternative Context ID: " + context['id'])
                return context['id']
            else:
                return False

    def fundamentalAccountingConcepts(self): 
        '''
        Get basic accounting data from 10-K or 10-Q filing.
        '''      
        self.logger.debug("Balance Sheet Date (document period end date): %s" % self.fields['BalanceSheetDate'])
        self.logger.debug("Income Statement Period (YTD, current period, period start date): %s to %s" % (self.fields['IncomeStatementPeriodYTD'], self.fields['BalanceSheetDate']))
        
        self.logger.debug("Context ID for document period focus (instants): %s" % self.fields['ContextForInstants'])
        self.logger.debug("Context ID for YTD period (durations): %s" % self.fields['ContextForDurations'])
        self.logger.debug(" ")

        
        #Assets
        self.fields['Assets'] = self.GetFactValue("us-gaap:Assets", "Instant")
        if self.fields['Assets']== None:
            self.fields['Assets'] = 0

        #Current Assets
        self.fields['CurrentAssets'] = self.GetFactValue("us-gaap:AssetsCurrent", "Instant")
        if self.fields['CurrentAssets']== None:
            self.fields['CurrentAssets'] = 0
                
        #Noncurrent Assets
        self.fields['NoncurrentAssets'] = self.GetFactValue("us-gaap:AssetsNoncurrent", "Instant")
        if self.fields['NoncurrentAssets']==None:
            if self.fields['Assets'] and self.fields['CurrentAssets']:
                self.fields['NoncurrentAssets'] = self.fields['Assets'] - self.fields['CurrentAssets']
            else:
                self.fields['NoncurrentAssets'] = 0
                
        #LiabilitiesAndEquity
        self.fields['LiabilitiesAndEquity'] = self.GetFactValue("us-gaap:LiabilitiesAndStockholdersEquity", "Instant")
        if self.fields['LiabilitiesAndEquity']== None:
            self.fields['LiabilitiesAndEquity'] = self.GetFactValue("us-gaap:LiabilitiesAndPartnersCapital", "Instant")
            if self.fields['LiabilitiesAndEquity']== None:
                self.fields['LiabilitiesAndEquity'] = 0
        
        #Liabilities
        self.fields['Liabilities'] = self.GetFactValue("us-gaap:Liabilities", "Instant")
        if self.fields['Liabilities']== None:
            self.fields['Liabilities'] = 0
                
        #CurrentLiabilities
        self.fields['CurrentLiabilities'] = self.GetFactValue("us-gaap:LiabilitiesCurrent", "Instant")
        if self.fields['CurrentLiabilities']== None:
            self.fields['CurrentLiabilities'] = 0
                
        #Noncurrent Liabilities
        self.fields['NoncurrentLiabilities'] = self.GetFactValue("us-gaap:LiabilitiesNoncurrent", "Instant")
        if self.fields['NoncurrentLiabilities']== None:
            if self.fields['Liabilities'] and self.fields['CurrentLiabilities']:
                self.fields['NoncurrentLiabilities'] = self.fields['Liabilities'] - self.fields['CurrentLiabilities']
            else:
                self.fields['NoncurrentLiabilities'] = 0
                
        #CommitmentsAndContingencies
        self.fields['CommitmentsAndContingencies'] = self.GetFactValue("us-gaap:CommitmentsAndContingencies", "Instant")
        if self.fields['CommitmentsAndContingencies']== None:
            self.fields['CommitmentsAndContingencies'] = 0
                
        #TemporaryEquity
        self.fields['TemporaryEquity'] = self.GetFactValue("us-gaap:TemporaryEquityRedemptionValue", "Instant")
        if self.fields['TemporaryEquity'] == None:
            self.fields['TemporaryEquity'] = self.GetFactValue("us-gaap:RedeemablePreferredStockCarryingAmount", "Instant")
            if self.fields['TemporaryEquity'] == None:
                self.fields['TemporaryEquity'] = self.GetFactValue("us-gaap:TemporaryEquityCarryingAmount", "Instant")
                if self.fields['TemporaryEquity'] == None:
                    self.fields['TemporaryEquity'] = self.GetFactValue("us-gaap:TemporaryEquityValueExcludingAdditionalPaidInCapital", "Instant")
                    if self.fields['TemporaryEquity'] == None:
                        self.fields['TemporaryEquity'] = self.GetFactValue("us-gaap:TemporaryEquityCarryingAmountAttributableToParent", "Instant")
                        if self.fields['TemporaryEquity'] == None:
                            self.fields['TemporaryEquity'] = self.GetFactValue("us-gaap:RedeemableNoncontrollingInterestEquityFairValue", "Instant")
                            if self.fields['TemporaryEquity'] == None:
                                self.fields['TemporaryEquity'] = 0
                    
        #RedeemableNoncontrollingInterest (added to temporary equity)
        RedeemableNoncontrollingInterest = None
        
        RedeemableNoncontrollingInterest = self.GetFactValue("us-gaap:RedeemableNoncontrollingInterestEquityCarryingAmount", "Instant")
        if RedeemableNoncontrollingInterest == None:
            RedeemableNoncontrollingInterest = self.GetFactValue("us-gaap:RedeemableNoncontrollingInterestEquityCommonCarryingAmount", "Instant")
            if RedeemableNoncontrollingInterest == None:
                RedeemableNoncontrollingInterest = 0

        #This adds redeemable noncontrolling interest and temporary equity which are rare, but can be reported seperately
        if self.fields['TemporaryEquity']:
            self.fields['TemporaryEquity'] = float(self.fields['TemporaryEquity']) + float(RedeemableNoncontrollingInterest)


        #Equity
        self.fields['Equity'] = self.GetFactValue("us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest", "Instant")
        if self.fields['Equity'] == None:
            self.fields['Equity'] = self.GetFactValue("us-gaap:StockholdersEquity", "Instant")
            if self.fields['Equity'] == None:
                self.fields['Equity'] = self.GetFactValue("us-gaap:PartnersCapitalIncludingPortionAttributableToNoncontrollingInterest", "Instant")
                if self.fields['Equity'] == None:
                    self.fields['Equity'] = self.GetFactValue("us-gaap:PartnersCapital", "Instant")
                    if self.fields['Equity'] == None:
                        self.fields['Equity'] = self.GetFactValue("us-gaap:CommonStockholdersEquity", "Instant")
                        if self.fields['Equity'] == None:
                            self.fields['Equity'] = self.GetFactValue("us-gaap:MemberEquity", "Instant")
                            if self.fields['Equity'] == None:
                                self.fields['Equity'] = self.GetFactValue("us-gaap:AssetsNet", "Instant")
                                if self.fields['Equity'] == None:
                                    self.fields['Equity'] = 0
        

        #EquityAttributableToNoncontrollingInterest
        self.fields['EquityAttributableToNoncontrollingInterest'] = self.GetFactValue("us-gaap:MinorityInterest", "Instant")
        if self.fields['EquityAttributableToNoncontrollingInterest'] == None:
            self.fields['EquityAttributableToNoncontrollingInterest'] = self.GetFactValue("us-gaap:PartnersCapitalAttributableToNoncontrollingInterest", "Instant")
            if self.fields['EquityAttributableToNoncontrollingInterest'] == None:
                self.fields['EquityAttributableToNoncontrollingInterest'] = 0
        
        #EquityAttributableToParent
        self.fields['EquityAttributableToParent'] = self.GetFactValue("us-gaap:StockholdersEquity", "Instant")
        if self.fields['EquityAttributableToParent'] == None:
            self.fields['EquityAttributableToParent'] = self.GetFactValue("us-gaap:LiabilitiesAndPartnersCapital", "Instant")
            if self.fields['EquityAttributableToParent'] == None:
                self.fields['EquityAttributableToParent'] = 0




        #BS Adjustments
        #if total assets is missing, try using current assets
        if self.fields['Assets'] == 0 and self.fields['Assets'] == self.fields['LiabilitiesAndEquity'] and self.fields['CurrentAssets'] == self.fields['LiabilitiesAndEquity']:
            self.fields['Assets'] = self.fields['CurrentAssets']
        
        #Added to fix Assets
        if self.fields['Assets'] == 0 and self.fields['LiabilitiesAndEquity'] != 0 and (self.fields['CurrentAssets'] == self.fields['LiabilitiesAndEquity']):
            self.fields['Assets'] = self.fields['CurrentAssets']
        
        #Added to fix Assets even more
        if self.fields['Assets'] == 0 and self.fields['NoncurrentAssets'] == 0 and self.fields['LiabilitiesAndEquity'] != 0 and (self.fields['LiabilitiesAndEquity']==self.fields['Liabilities']+self.fields['Equity']):
            self.fields['Assets'] = self.fields['CurrentAssets']
        
        if self.fields['Assets']!=0 and self.fields['CurrentAssets']!=0:
            self.fields['NoncurrentAssets'] = self.fields['Assets'] - self.fields['CurrentAssets']
        
        if self.fields['LiabilitiesAndEquity']==0 and self.fields['Assets']!=0:
            self.fields['LiabilitiesAndEquity'] = self.fields['Assets']
        
        #Impute: Equity based no parent and noncontrolling interest being present
        if self.fields['EquityAttributableToNoncontrollingInterest']!=0 and self.fields['EquityAttributableToParent']!=0:
            self.fields['Equity'] = self.fields['EquityAttributableToParent'] + self.fields['EquityAttributableToNoncontrollingInterest']
        
        if self.fields['Equity']==0 and self.fields['EquityAttributableToNoncontrollingInterest']==0 and self.fields['EquityAttributableToParent']!=0:
            self.fields['Equity'] = self.fields['EquityAttributableToParent']
        
        if self.fields['Equity']==0:
            self.fields['Equity'] = self.fields['EquityAttributableToParent'] + self.fields['EquityAttributableToNoncontrollingInterest']
        
        #Added: Impute Equity attributable to parent based on existence of equity and noncontrolling interest.
        if self.fields['Equity']!=0 and self.fields['EquityAttributableToNoncontrollingInterest']!=0 and self.fields['EquityAttributableToParent']==0:
            self.fields['EquityAttributableToParent'] = self.fields['Equity'] - self.fields['EquityAttributableToNoncontrollingInterest']
        
        #Added: Impute Equity attributable to parent based on existence of equity and noncontrolling interest.
        if self.fields['Equity']!=0 and self.fields['EquityAttributableToNoncontrollingInterest']==0 and self.fields['EquityAttributableToParent']==0:
            self.fields['EquityAttributableToParent'] = self.fields['Equity']
        
        #if total liabilities is missing, figure it out based on liabilities and equity
        if self.fields['Liabilities']==0 and self.fields['Equity']!=0:
            self.fields['Liabilities'] = self.fields['LiabilitiesAndEquity'] - (self.fields['CommitmentsAndContingencies'] + self.fields['TemporaryEquity'] + self.fields['Equity'])
        
        #This seems incorrect because liabilities might not be reported
        if self.fields['Liabilities']!=0 and self.fields['CurrentLiabilities']!=0:
            self.fields['NoncurrentLiabilities'] = self.fields['Liabilities'] - self.fields['CurrentLiabilities']
        
        #Added to fix liabilities based on current liabilities
        if self.fields['Liabilities']==0 and self.fields['CurrentLiabilities']!=0 and self.fields['NoncurrentLiabilities']==0:
            self.fields['Liabilities'] = self.fields['CurrentLiabilities']
        
                    
        lngBSCheck1 = self.fields['Equity'] - (self.fields['EquityAttributableToParent'] + self.fields['EquityAttributableToNoncontrollingInterest'])
        lngBSCheck2 = self.fields['Assets'] - self.fields['LiabilitiesAndEquity']
        
        if self.fields['CurrentAssets']==0 and self.fields['NoncurrentAssets']==0 and self.fields['CurrentLiabilities']==0 and self.fields['NoncurrentLiabilities']==0:
            #if current assets/liabilities are zero and noncurrent assets/liabilities;: don't do this test because the balance sheet is not classified
            lngBSCheck3 = 0
            lngBSCheck4 = 0
        
        else:
            #balance sheet IS classified
            lngBSCheck3 = self.fields['Assets'] - (self.fields['CurrentAssets'] + self.fields['NoncurrentAssets'])
            lngBSCheck4 = self.fields['Liabilities'] - (self.fields['CurrentLiabilities'] + self.fields['NoncurrentLiabilities'])
        
        
        lngBSCheck5 = self.fields['LiabilitiesAndEquity'] - (self.fields['Liabilities'] + self.fields['CommitmentsAndContingencies'] + self.fields['TemporaryEquity'] + self.fields['Equity'])
        
        if lngBSCheck1:
            self.logger.debug("BS1: Equity(" + str(self.fields['Equity']) + ") = EquityAttributableToParent(" + str(self.fields['EquityAttributableToParent']) + ") , EquityAttributableToNoncontrollingInterest(" + str(self.fields['EquityAttributableToNoncontrollingInterest']) + "): " + str(lngBSCheck1))
        if lngBSCheck2:
            self.logger.debug("BS2: Assets(" + str(self.fields['Assets']) + ") = LiabilitiesAndEquity(" + str(self.fields['LiabilitiesAndEquity']) + "): " + str(lngBSCheck2))
        if lngBSCheck3:
            self.logger.debug("BS3: Assets(" + str(self.fields['Assets']) + ") = CurrentAssets(" + str(self.fields['CurrentAssets']) + ") , NoncurrentAssets(" + str(self.fields['NoncurrentAssets']) + "): " + str(lngBSCheck3))
        if lngBSCheck4:
            self.logger.debug("BS4: Liabilities(" + str(self.fields['Liabilities']) + ")= CurrentLiabilities(" + str(self.fields['CurrentLiabilities']) + ") , NoncurrentLiabilities(" + str(self.fields['NoncurrentLiabilities']) + "): " + str(lngBSCheck4))
        if lngBSCheck5:
            self.logger.debug("BS5: Liabilities and Equity(" + str(self.fields['LiabilitiesAndEquity']) + ")= Liabilities(" + str(self.fields['Liabilities']) + ") , CommitmentsAndContingencies(" + str(self.fields['CommitmentsAndContingencies']) + "), TemporaryEquity(" + str(self.fields['TemporaryEquity']) + "), Equity(" + str(self.fields['Equity']) + "): " + str(lngBSCheck5))
                
        

        #Income statement

        #Revenues
        self.fields['Revenues'] = self.GetFactValue("us-gaap:Revenues", "Duration")
        if self.fields['Revenues'] == None:
            self.fields['Revenues'] = self.GetFactValue("us-gaap:SalesRevenueNet", "Duration")
            if self.fields['Revenues'] == None:
                self.fields['Revenues'] = self.GetFactValue("us-gaap:SalesRevenueServicesNet", "Duration")
                if self.fields['Revenues'] == None:
                    self.fields['Revenues'] = self.GetFactValue("us-gaap:RevenuesNetOfInterestExpense", "Duration")
                    if self.fields['Revenues'] == None:
                        self.fields['Revenues'] = self.GetFactValue("us-gaap:RegulatedAndUnregulatedOperatingRevenue", "Duration")
                        if self.fields['Revenues'] == None:
                            self.fields['Revenues'] = self.GetFactValue("us-gaap:HealthCareOrganizationRevenue", "Duration")
                            if self.fields['Revenues'] == None:
                                self.fields['Revenues'] = self.GetFactValue("us-gaap:InterestAndDividendIncomeOperating", "Duration")
                                if self.fields['Revenues'] == None:
                                    self.fields['Revenues'] = self.GetFactValue("us-gaap:RealEstateRevenueNet", "Duration")
                                    if self.fields['Revenues'] == None:
                                        self.fields['Revenues'] = self.GetFactValue("us-gaap:RevenueMineralSales", "Duration")
                                        if self.fields['Revenues'] == None:
                                            self.fields['Revenues'] = self.GetFactValue("us-gaap:OilAndGasRevenue", "Duration")
                                            if self.fields['Revenues'] == None:
                                                self.fields['Revenues'] = self.GetFactValue("us-gaap:FinancialServicesRevenue", "Duration")
                                                if self.fields['Revenues'] == None:
                                                    self.fields['Revenues'] = self.GetFactValue("us-gaap:RegulatedAndUnregulatedOperatingRevenue", "Duration")                                                
                                                    if self.fields['Revenues'] == None:
                                                        self.fields['Revenues'] = 0


        #CostOfRevenue
        self.fields['CostOfRevenue'] = self.GetFactValue("us-gaap:CostOfRevenue", "Duration")
        if self.fields['CostOfRevenue'] == None:
            self.fields['CostOfRevenue'] = self.GetFactValue("us-gaap:CostOfServices", "Duration")
            if self.fields['CostOfRevenue'] == None:
                self.fields['CostOfRevenue'] = self.GetFactValue("us-gaap:CostOfGoodsSold", "Duration")
                if self.fields['CostOfRevenue'] == None:
                    self.fields['CostOfRevenue'] = self.GetFactValue("us-gaap:CostOfGoodsAndServicesSold", "Duration")
                    if self.fields['CostOfRevenue'] == None:
                        self.fields['CostOfRevenue'] = 0
        
        #GrossProfit
        self.fields['GrossProfit'] = self.GetFactValue("us-gaap:GrossProfit", "Duration")
        if self.fields['GrossProfit'] == None:
            self.fields['GrossProfit'] = self.GetFactValue("us-gaap:GrossProfit", "Duration")
            if self.fields['GrossProfit'] == None:
                self.fields['GrossProfit'] = 0
        
        #OperatingExpenses
        self.fields['OperatingExpenses'] = self.GetFactValue("us-gaap:OperatingExpenses", "Duration")
        if self.fields['OperatingExpenses'] == None:
            self.fields['OperatingExpenses'] = self.GetFactValue("us-gaap:OperatingCostsAndExpenses", "Duration")  #This concept seems incorrect.
            if self.fields['OperatingExpenses'] == None:
                self.fields['OperatingExpenses'] = 0

        #CostsAndExpenses
        self.fields['CostsAndExpenses'] = self.GetFactValue("us-gaap:CostsAndExpenses", "Duration")
        if self.fields['CostsAndExpenses'] == None:
            self.fields['CostsAndExpenses'] = self.GetFactValue("us-gaap:CostsAndExpenses", "Duration")
            if self.fields['CostsAndExpenses'] == None:
                self.fields['CostsAndExpenses'] = 0
        
        #OtherOperatingIncome
        self.fields['OtherOperatingIncome'] = self.GetFactValue("us-gaap:OtherOperatingIncome", "Duration")
        if self.fields['OtherOperatingIncome'] == None:
            self.fields['OtherOperatingIncome'] = self.GetFactValue("us-gaap:OtherOperatingIncome", "Duration")
            if self.fields['OtherOperatingIncome'] == None:
                self.fields['OtherOperatingIncome'] = 0
        
        #OperatingIncomeLoss
        self.fields['OperatingIncomeLoss'] = self.GetFactValue("us-gaap:OperatingIncomeLoss", "Duration")
        if self.fields['OperatingIncomeLoss'] == None:
            self.fields['OperatingIncomeLoss'] = self.GetFactValue("us-gaap:OperatingIncomeLoss", "Duration")
            if self.fields['OperatingIncomeLoss'] == None:
                self.fields['OperatingIncomeLoss'] = 0
        
        #NonoperatingIncomeLoss
        self.fields['NonoperatingIncomeLoss'] = self.GetFactValue("us-gaap:NonoperatingIncomeExpense", "Duration")
        if self.fields['NonoperatingIncomeLoss'] == None:
            self.fields['NonoperatingIncomeLoss'] = self.GetFactValue("us-gaap:NonoperatingIncomeExpense", "Duration")
            if self.fields['NonoperatingIncomeLoss'] == None:
                self.fields['NonoperatingIncomeLoss'] = 0

        #InterestAndDebtExpense
        self.fields['InterestAndDebtExpense'] = self.GetFactValue("us-gaap:InterestAndDebtExpense", "Duration")
        if self.fields['InterestAndDebtExpense'] == None:
            self.fields['InterestAndDebtExpense'] = self.GetFactValue("us-gaap:InterestAndDebtExpense", "Duration")
            if self.fields['InterestAndDebtExpense'] == None:
                self.fields['InterestAndDebtExpense'] = 0

        #IncomeBeforeEquityMethodInvestments
        self.fields['IncomeBeforeEquityMethodInvestments'] = self.GetFactValue("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments", "Duration")
        if self.fields['IncomeBeforeEquityMethodInvestments'] == None:
            self.fields['IncomeBeforeEquityMethodInvestments'] = self.GetFactValue("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments", "Duration")
            if self.fields['IncomeBeforeEquityMethodInvestments'] == None:
                self.fields['IncomeBeforeEquityMethodInvestments'] = 0
        
        #IncomeFromEquityMethodInvestments
        self.fields['IncomeFromEquityMethodInvestments'] = self.GetFactValue("us-gaap:IncomeLossFromEquityMethodInvestments", "Duration")
        if self.fields['IncomeFromEquityMethodInvestments'] == None:
            self.fields['IncomeFromEquityMethodInvestments'] = self.GetFactValue("us-gaap:IncomeLossFromEquityMethodInvestments", "Duration")
            if self.fields['IncomeFromEquityMethodInvestments'] == None:
                self.fields['IncomeFromEquityMethodInvestments'] = 0
        
        #IncomeFromContinuingOperationsBeforeTax
        self.fields['IncomeFromContinuingOperationsBeforeTax'] = self.GetFactValue("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments", "Duration")
        if self.fields['IncomeFromContinuingOperationsBeforeTax'] == None:
            self.fields['IncomeFromContinuingOperationsBeforeTax'] = self.GetFactValue("us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", "Duration")
            if self.fields['IncomeFromContinuingOperationsBeforeTax'] == None:
                self.fields['IncomeFromContinuingOperationsBeforeTax'] = 0
        
        #IncomeTaxExpenseBenefit
        self.fields['IncomeTaxExpenseBenefit'] = self.GetFactValue("us-gaap:IncomeTaxExpenseBenefit", "Duration")
        if self.fields['IncomeTaxExpenseBenefit'] == None:
            self.fields['IncomeTaxExpenseBenefit'] = self.GetFactValue("us-gaap:IncomeTaxExpenseBenefitContinuingOperations", "Duration")
            if self.fields['IncomeTaxExpenseBenefit'] == None:
                self.fields['IncomeTaxExpenseBenefit'] = 0
        
        #IncomeFromContinuingOperationsAfterTax
        self.fields['IncomeFromContinuingOperationsAfterTax'] = self.GetFactValue("us-gaap:IncomeLossBeforeExtraordinaryItemsAndCumulativeEffectOfChangeInAccountingPrinciple", "Duration")
        if self.fields['IncomeFromContinuingOperationsAfterTax'] == None:
            self.fields['IncomeFromContinuingOperationsAfterTax'] = self.GetFactValue("us-gaap:IncomeLossBeforeExtraordinaryItemsAndCumulativeEffectOfChangeInAccountingPrinciple", "Duration")
            if self.fields['IncomeFromContinuingOperationsAfterTax'] == None:
                self.fields['IncomeFromContinuingOperationsAfterTax'] = 0

        #IncomeFromDiscontinuedOperations
        self.fields['IncomeFromDiscontinuedOperations'] = self.GetFactValue("us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax", "Duration")
        if self.fields['IncomeFromDiscontinuedOperations']== None:
            self.fields['IncomeFromDiscontinuedOperations'] = self.GetFactValue("us-gaap:DiscontinuedOperationGainLossOnDisposalOfDiscontinuedOperationNetOfTax", "Duration")
            if self.fields['IncomeFromDiscontinuedOperations']== None:
                self.fields['IncomeFromDiscontinuedOperations'] = self.GetFactValue("us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTaxAttributableToReportingEntity", "Duration")
                if self.fields['IncomeFromDiscontinuedOperations']== None:
                    self.fields['IncomeFromDiscontinuedOperations'] = 0

        #ExtraordaryItemsGainLoss
        self.fields['ExtraordaryItemsGainLoss'] = self.GetFactValue("us-gaap:ExtraordinaryItemNetOfTax", "Duration")
        if self.fields['ExtraordaryItemsGainLoss']== None:
            self.fields['ExtraordaryItemsGainLoss'] = self.GetFactValue("us-gaap:ExtraordinaryItemNetOfTax", "Duration")
            if self.fields['ExtraordaryItemsGainLoss']== None:
                self.fields['ExtraordaryItemsGainLoss'] = 0

        #NetIncomeLoss
        self.fields['NetIncomeLoss'] = self.GetFactValue("us-gaap:ProfitLoss", "Duration")
        if self.fields['NetIncomeLoss']== None:
            self.fields['NetIncomeLoss'] = self.GetFactValue("us-gaap:NetIncomeLoss", "Duration")
            if self.fields['NetIncomeLoss']== None:
                self.fields['NetIncomeLoss'] = self.GetFactValue("us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic", "Duration")
                if self.fields['NetIncomeLoss']== None:
                    self.fields['NetIncomeLoss'] = self.GetFactValue("us-gaap:IncomeLossFromContinuingOperations", "Duration")
                    if self.fields['NetIncomeLoss']== None:
                        self.fields['NetIncomeLoss'] = self.GetFactValue("us-gaap:IncomeLossAttributableToParent", "Duration")
                        if self.fields['NetIncomeLoss']== None:
                            self.fields['NetIncomeLoss'] = self.GetFactValue("us-gaap:IncomeLossFromContinuingOperationsIncludingPortionAttributableToNoncontrollingInterest", "Duration")
                            if self.fields['NetIncomeLoss']== None:
                                self.fields['NetIncomeLoss'] = 0

        #NetIncomeAvailableToCommonStockholdersBasic
        self.fields['NetIncomeAvailableToCommonStockholdersBasic'] = self.GetFactValue("us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic", "Duration")
        if self.fields['NetIncomeAvailableToCommonStockholdersBasic']== None:
            self.fields['NetIncomeAvailableToCommonStockholdersBasic'] = 0
                
        #PreferredStockDividendsAndOtherAdjustments
        self.fields['PreferredStockDividendsAndOtherAdjustments'] = self.GetFactValue("us-gaap:PreferredStockDividendsAndOtherAdjustments", "Duration")
        if self.fields['PreferredStockDividendsAndOtherAdjustments']== None:
            self.fields['PreferredStockDividendsAndOtherAdjustments'] = 0
                
        #NetIncomeAttributableToNoncontrollingInterest
        self.fields['NetIncomeAttributableToNoncontrollingInterest'] = self.GetFactValue("us-gaap:NetIncomeLossAttributableToNoncontrollingInterest", "Duration")
        if self.fields['NetIncomeAttributableToNoncontrollingInterest']== None:
            self.fields['NetIncomeAttributableToNoncontrollingInterest'] = 0
                
        #NetIncomeAttributableToParent
        self.fields['NetIncomeAttributableToParent'] = self.GetFactValue("us-gaap:NetIncomeLoss", "Duration")
        if self.fields['NetIncomeAttributableToParent']== None:
            self.fields['NetIncomeAttributableToParent'] = 0

        #OtherComprehensiveIncome
        self.fields['OtherComprehensiveIncome'] = self.GetFactValue("us-gaap:OtherComprehensiveIncomeLossNetOfTax", "Duration")
        if self.fields['OtherComprehensiveIncome']== None:
            self.fields['OtherComprehensiveIncome'] = self.GetFactValue("us-gaap:OtherComprehensiveIncomeLossNetOfTax", "Duration")
            if self.fields['OtherComprehensiveIncome']== None:
                self.fields['OtherComprehensiveIncome'] = 0

        #ComprehensiveIncome
        self.fields['ComprehensiveIncome'] = self.GetFactValue("us-gaap:ComprehensiveIncomeNetOfTaxIncludingPortionAttributableToNoncontrollingInterest", "Duration")
        if self.fields['ComprehensiveIncome']== None:
            self.fields['ComprehensiveIncome'] = self.GetFactValue("us-gaap:ComprehensiveIncomeNetOfTax", "Duration")
            if self.fields['ComprehensiveIncome']== None:
                self.fields['ComprehensiveIncome'] = 0

        #ComprehensiveIncomeAttributableToParent
        self.fields['ComprehensiveIncomeAttributableToParent'] = self.GetFactValue("us-gaap:ComprehensiveIncomeNetOfTax", "Duration")
        if self.fields['ComprehensiveIncomeAttributableToParent']== None:
            self.fields['ComprehensiveIncomeAttributableToParent'] = self.GetFactValue("us-gaap:ComprehensiveIncomeNetOfTax", "Duration")
            if self.fields['ComprehensiveIncomeAttributableToParent']== None:
                self.fields['ComprehensiveIncomeAttributableToParent'] = 0
        
        #ComprehensiveIncomeAttributableToNoncontrollingInterest
        self.fields['ComprehensiveIncomeAttributableToNoncontrollingInterest'] = self.GetFactValue("us-gaap:ComprehensiveIncomeNetOfTaxAttributableToNoncontrollingInterest", "Duration")
        if self.fields['ComprehensiveIncomeAttributableToNoncontrollingInterest']==None:
            self.fields['ComprehensiveIncomeAttributableToNoncontrollingInterest'] = self.GetFactValue("us-gaap:ComprehensiveIncomeNetOfTaxAttributableToNoncontrollingInterest", "Duration")
            if self.fields['ComprehensiveIncomeAttributableToNoncontrollingInterest']==None:
                self.fields['ComprehensiveIncomeAttributableToNoncontrollingInterest'] = 0



        #########'Adjustments to income statement information
        #Impute: NonoperatingIncomeLossPlusInterestAndDebtExpense
        self.fields['NonoperatingIncomeLossPlusInterestAndDebtExpense'] = self.fields['NonoperatingIncomeLoss'] + self.fields['InterestAndDebtExpense']

        #Impute: Net income available to common stockholders  (if it does not exist)
        if self.fields['NetIncomeAvailableToCommonStockholdersBasic']==0 and self.fields['PreferredStockDividendsAndOtherAdjustments']==0 and self.fields['NetIncomeAttributableToParent']!=0:
            self.fields['NetIncomeAvailableToCommonStockholdersBasic'] = self.fields['NetIncomeAttributableToParent']
                
        #Impute NetIncomeLoss
        if self.fields['NetIncomeLoss']!=0 and self.fields['IncomeFromContinuingOperationsAfterTax']==0:
            self.fields['IncomeFromContinuingOperationsAfterTax'] = self.fields['NetIncomeLoss'] - self.fields['IncomeFromDiscontinuedOperations'] - self.fields['ExtraordaryItemsGainLoss']

        #Impute: Net income attributable to parent if it does not exist
        if self.fields['NetIncomeAttributableToParent']==0 and self.fields['NetIncomeAttributableToNoncontrollingInterest']==0 and self.fields['NetIncomeLoss']!=0:
            self.fields['NetIncomeAttributableToParent'] = self.fields['NetIncomeLoss']

        #Impute: PreferredStockDividendsAndOtherAdjustments
        if self.fields['PreferredStockDividendsAndOtherAdjustments']==0 and self.fields['NetIncomeAttributableToParent']!=0 and self.fields['NetIncomeAvailableToCommonStockholdersBasic']!=0:
            self.fields['PreferredStockDividendsAndOtherAdjustments'] = self.fields['NetIncomeAttributableToParent'] - self.fields['NetIncomeAvailableToCommonStockholdersBasic']

        #Impute: comprehensive income
        if self.fields['ComprehensiveIncomeAttributableToParent']==0 and self.fields['ComprehensiveIncomeAttributableToNoncontrollingInterest']==0 and self.fields['ComprehensiveIncome']==0 and self.fields['OtherComprehensiveIncome']==0:
            self.fields['ComprehensiveIncome'] = self.fields['NetIncomeLoss']
                
        #Impute: other comprehensive income
        if self.fields['ComprehensiveIncome']!=0 and self.fields['OtherComprehensiveIncome']==0:
            self.fields['OtherComprehensiveIncome'] = self.fields['ComprehensiveIncome'] - self.fields['NetIncomeLoss']

        #Impute: comprehensive income attributable to parent if it does not exist
        if self.fields['ComprehensiveIncomeAttributableToParent']==0 and self.fields['ComprehensiveIncomeAttributableToNoncontrollingInterest']==0 and self.fields['ComprehensiveIncome']!=0:
            self.fields['ComprehensiveIncomeAttributableToParent'] = self.fields['ComprehensiveIncome']

        #Impute: IncomeFromContinuingOperations*Before*Tax
        if self.fields['IncomeBeforeEquityMethodInvestments']!=0 and self.fields['IncomeFromEquityMethodInvestments']!=0 and self.fields['IncomeFromContinuingOperationsBeforeTax']==0:
            self.fields['IncomeFromContinuingOperationsBeforeTax'] = self.fields['IncomeBeforeEquityMethodInvestments'] + self.fields['IncomeFromEquityMethodInvestments']
                
        #Impute: IncomeFromContinuingOperations*Before*Tax2 (if income before tax is missing)
        if self.fields['IncomeFromContinuingOperationsBeforeTax']==0 and self.fields['IncomeFromContinuingOperationsAfterTax']!=0:
            self.fields['IncomeFromContinuingOperationsBeforeTax'] = self.fields['IncomeFromContinuingOperationsAfterTax'] + self.fields['IncomeTaxExpenseBenefit']
                
        #Impute: IncomeFromContinuingOperations*After*Tax
        if self.fields['IncomeFromContinuingOperationsAfterTax']==0 and \
            (self.fields['IncomeTaxExpenseBenefit']!=0 or self.fields['IncomeTaxExpenseBenefit']==0) and self.fields['IncomeFromContinuingOperationsBeforeTax']!=0:
            self.fields['IncomeFromContinuingOperationsAfterTax'] = self.fields['IncomeFromContinuingOperationsBeforeTax'] - self.fields['IncomeTaxExpenseBenefit']
                
                
        #Impute: GrossProfit
        if self.fields['GrossProfit']==0 and (self.fields['Revenues']!=0 and self.fields['CostOfRevenue']!=0):
            self.fields['GrossProfit'] = self.fields['Revenues'] - self.fields['CostOfRevenue']
                
        #Impute: GrossProfit
        if self.fields['GrossProfit']==0 and (self.fields['Revenues']!=0 and self.fields['CostOfRevenue']!=0):
            self.fields['GrossProfit'] = self.fields['Revenues'] - self.fields['CostOfRevenue']
                
        #Impute: Revenues
        if self.fields['GrossProfit']!=0 and (self.fields['Revenues']==0 and self.fields['CostOfRevenue']!=0):
            self.fields['Revenues'] = self.fields['GrossProfit'] + self.fields['CostOfRevenue']
                
        #Impute: CostOfRevenue
        if self.fields['GrossProfit']!=0 and (self.fields['Revenues']!=0 and self.fields['CostOfRevenue']==0):
            self.fields['CostOfRevenue'] = self.fields['GrossProfit'] + self.fields['Revenues']
        
        #Impute: CostsAndExpenses (would NEVER have costs and expenses if has gross profit, gross profit is multi-step and costs and expenses is single-step)
        if self.fields['GrossProfit']==0 and self.fields['CostsAndExpenses']==0 and (self.fields['CostOfRevenue']!=0 and self.fields['OperatingExpenses']!=0):
            self.fields['CostsAndExpenses'] = self.fields['CostOfRevenue'] + self.fields['OperatingExpenses']
                
        #Impute: CostsAndExpenses based on existance of both costs of revenues and operating expenses
        if self.fields['CostsAndExpenses']==0 and self.fields['OperatingExpenses']!=0 and (self.fields['CostOfRevenue']!=0):
            self.fields['CostsAndExpenses'] = self.fields['CostOfRevenue'] + self.fields['OperatingExpenses']
                
        #Impute: CostsAndExpenses
        if self.fields['GrossProfit']==0 and self.fields['CostsAndExpenses']==0 and self.fields['Revenues']!=0 and self.fields['OperatingIncomeLoss']!=0 and self.fields['OtherOperatingIncome']!=0:
            self.fields['CostsAndExpenses'] = self.fields['Revenues'] - self.fields['OperatingIncomeLoss'] - self.fields['OtherOperatingIncome']
                
        #Impute: OperatingExpenses based on existance of costs and expenses and cost of revenues
        if self.fields['CostOfRevenue']!=0 and self.fields['CostsAndExpenses']!=0 and self.fields['OperatingExpenses']==0:
            self.fields['OperatingExpenses'] = self.fields['CostsAndExpenses'] - self.fields['CostOfRevenue']
                
        #Impute: CostOfRevenues single-step method
        if self.fields['Revenues']!=0 and self.fields['GrossProfit']==0 and \
            (self.fields['Revenues'] - self.fields['CostsAndExpenses']==self.fields['OperatingIncomeLoss']) and \
            self.fields['OperatingExpenses']==0 and self.fields['OtherOperatingIncome']==0:
            self.fields['CostOfRevenue'] = self.fields['CostsAndExpenses'] - self.fields['OperatingExpenses']

        #Impute: IncomeBeforeEquityMethodInvestments
        if self.fields['IncomeBeforeEquityMethodInvestments']==0 and self.fields['IncomeFromContinuingOperationsBeforeTax']!=0:
            self.fields['IncomeBeforeEquityMethodInvestments'] = self.fields['IncomeFromContinuingOperationsBeforeTax'] - self.fields['IncomeFromEquityMethodInvestments']
                
        #Impute: IncomeBeforeEquityMethodInvestments
        if self.fields['OperatingIncomeLoss']!=0 and (self.fields['NonoperatingIncomeLoss']!=0 and \
            self.fields['InterestAndDebtExpense']==0 and self.fields['IncomeBeforeEquityMethodInvestments']!=0):
            self.fields['InterestAndDebtExpense'] = self.fields['IncomeBeforeEquityMethodInvestments'] - (self.fields['OperatingIncomeLoss'] + self.fields['NonoperatingIncomeLoss'])
        
        #Impute: OtherOperatingIncome
        if self.fields['GrossProfit']!=0 and (self.fields['OperatingExpenses']!=0 and self.fields['OperatingIncomeLoss']!=0):
            self.fields['OtherOperatingIncome'] = self.fields['OperatingIncomeLoss'] - (self.fields['GrossProfit'] - self.fields['OperatingExpenses'])

        #Move IncomeFromEquityMethodInvestments
        if self.fields['IncomeFromEquityMethodInvestments']!=0 and \
            self.fields['IncomeBeforeEquityMethodInvestments']!=0 and self.fields['IncomeBeforeEquityMethodInvestments']!=self.fields['IncomeFromContinuingOperationsBeforeTax']:
            self.fields['IncomeBeforeEquityMethodInvestments'] = self.fields['IncomeFromContinuingOperationsBeforeTax'] - self.fields['IncomeFromEquityMethodInvestments']
            self.fields['OperatingIncomeLoss'] = self.fields['OperatingIncomeLoss'] - self.fields['IncomeFromEquityMethodInvestments']
        
        #DANGEROUS!!  May need to turn off. IS3 had 2085 PASSES WITHOUT this imputing. if it is higher,: keep the test
        #Impute: OperatingIncomeLoss
        if self.fields['OperatingIncomeLoss']==0 and self.fields['IncomeBeforeEquityMethodInvestments']!=0:
            self.fields['OperatingIncomeLoss'] = self.fields['IncomeBeforeEquityMethodInvestments'] + self.fields['NonoperatingIncomeLoss'] - self.fields['InterestAndDebtExpense']
                
        
        self.fields['NonoperatingIncomePlusInterestAndDebtExpensePlusIncomeFromEquityMethodInvestments'] = self.fields['IncomeFromContinuingOperationsBeforeTax'] - self.fields['OperatingIncomeLoss']

        #NonoperatingIncomeLossPlusInterestAndDebtExpense
        if self.fields['NonoperatingIncomeLossPlusInterestAndDebtExpense']== 0 and self.fields['NonoperatingIncomePlusInterestAndDebtExpensePlusIncomeFromEquityMethodInvestments']!=0:
            self.fields['NonoperatingIncomeLossPlusInterestAndDebtExpense'] = self.fields['NonoperatingIncomePlusInterestAndDebtExpensePlusIncomeFromEquityMethodInvestments'] - self.fields['IncomeFromEquityMethodInvestments']

        
                
        lngIS1 = (self.fields['Revenues'] - self.fields['CostOfRevenue']) - self.fields['GrossProfit']
        lngIS2 = (self.fields['GrossProfit'] - self.fields['OperatingExpenses'] + self.fields['OtherOperatingIncome']) - self.fields['OperatingIncomeLoss']
        lngIS3 = (self.fields['OperatingIncomeLoss'] + self.fields['NonoperatingIncomeLossPlusInterestAndDebtExpense']) - self.fields['IncomeBeforeEquityMethodInvestments']
        lngIS4 = (self.fields['IncomeBeforeEquityMethodInvestments'] + self.fields['IncomeFromEquityMethodInvestments']) - self.fields['IncomeFromContinuingOperationsBeforeTax']
        lngIS5 = (self.fields['IncomeFromContinuingOperationsBeforeTax'] - self.fields['IncomeTaxExpenseBenefit']) - self.fields['IncomeFromContinuingOperationsAfterTax']
        lngIS6 = (self.fields['IncomeFromContinuingOperationsAfterTax'] + self.fields['IncomeFromDiscontinuedOperations'] + self.fields['ExtraordaryItemsGainLoss']) - self.fields['NetIncomeLoss']
        lngIS7 = (self.fields['NetIncomeAttributableToParent'] + self.fields['NetIncomeAttributableToNoncontrollingInterest']) - self.fields['NetIncomeLoss']
        lngIS8 = (self.fields['NetIncomeAttributableToParent'] - self.fields['PreferredStockDividendsAndOtherAdjustments']) - self.fields['NetIncomeAvailableToCommonStockholdersBasic']
        lngIS9 = (self.fields['ComprehensiveIncomeAttributableToParent'] + self.fields['ComprehensiveIncomeAttributableToNoncontrollingInterest']) - self.fields['ComprehensiveIncome']
        lngIS10 = (self.fields['NetIncomeLoss'] + self.fields['OtherComprehensiveIncome']) - self.fields['ComprehensiveIncome']
        lngIS11 = self.fields['OperatingIncomeLoss'] - (self.fields['Revenues'] - self.fields['CostsAndExpenses'] + self.fields['OtherOperatingIncome'])
        
        if lngIS1:
            self.logger.debug("IS1: GrossProfit(" + str(self.fields['GrossProfit']) + ") = Revenues(" + str(self.fields['Revenues']) + ") - CostOfRevenue(" + str(self.fields['CostOfRevenue']) + "): " + str(lngIS1))
        if lngIS2:
            self.logger.debug("IS2: OperatingIncomeLoss(" + str(self.fields['OperatingIncomeLoss']) + ") = GrossProfit(" + str(self.fields['GrossProfit']) + ") - OperatingExpenses(" + str(self.fields['OperatingExpenses']) + ") , OtherOperatingIncome(" + str(self.fields['OtherOperatingIncome']) + "): " + str(lngIS2))
        if lngIS3:        
            self.logger.debug("IS3: IncomeBeforeEquityMethodInvestments(" + str(self.fields['IncomeBeforeEquityMethodInvestments']) + ") = OperatingIncomeLoss(" + str(self.fields['OperatingIncomeLoss']) + ") - NonoperatingIncomeLoss(" + str(self.fields['NonoperatingIncomeLoss']) + "), InterestAndDebtExpense(" + str(self.fields['InterestAndDebtExpense']) + "): " + str(lngIS3))
        if lngIS4:
            self.logger.debug("IS4: IncomeFromContinuingOperationsBeforeTax(" + str(self.fields['IncomeFromContinuingOperationsBeforeTax']) + ") = IncomeBeforeEquityMethodInvestments(" + str(self.fields['IncomeBeforeEquityMethodInvestments']) + ") , IncomeFromEquityMethodInvestments(" + str(self.fields['IncomeFromEquityMethodInvestments']) + "): " + str(lngIS4))
        
        if lngIS5:
            self.logger.debug("IS5: IncomeFromContinuingOperationsAfterTax(" + str(self.fields['IncomeFromContinuingOperationsAfterTax']) + ") = IncomeFromContinuingOperationsBeforeTax(" + str(self.fields['IncomeFromContinuingOperationsBeforeTax']) + ") - IncomeTaxExpenseBenefit(" + str(self.fields['IncomeTaxExpenseBenefit']) + "): " + str(lngIS5))
        if  lngIS6:
            self.logger.debug("IS6: NetIncomeLoss(" + str(self.fields['NetIncomeLoss']) + ") = IncomeFromContinuingOperationsAfterTax(" + str(self.fields['IncomeFromContinuingOperationsAfterTax']) + ") , IncomeFromDiscontinuedOperations(" + str(self.fields['IncomeFromDiscontinuedOperations']) + ") , ExtraordaryItemsGainLoss(" + str(self.fields['ExtraordaryItemsGainLoss']) + "): " + str(lngIS6))
        if lngIS7:
            self.logger.debug("IS7: NetIncomeLoss(" + str(self.fields['NetIncomeLoss']) + ") = NetIncomeAttributableToParent(" + str(self.fields['NetIncomeAttributableToParent']) + ") , NetIncomeAttributableToNoncontrollingInterest(" + str(self.fields['NetIncomeAttributableToNoncontrollingInterest']) + "): " + str(lngIS7))
        if lngIS8:
            self.logger.debug("IS8: NetIncomeAvailableToCommonStockholdersBasic(" + str(self.fields['NetIncomeAvailableToCommonStockholdersBasic']) + ") = NetIncomeAttributableToParent(" + str(self.fields['NetIncomeAttributableToParent']) + ") - PreferredStockDividendsAndOtherAdjustments(" + str(self.fields['PreferredStockDividendsAndOtherAdjustments']) + "): " + str(lngIS8))
        if lngIS9:
            self.logger.debug("IS9: ComprehensiveIncome(" + str(self.fields['ComprehensiveIncome']) + ") = ComprehensiveIncomeAttributableToParent(" + str(self.fields['ComprehensiveIncomeAttributableToParent']) + ") , ComprehensiveIncomeAttributableToNoncontrollingInterest(" + str(self.fields['ComprehensiveIncomeAttributableToNoncontrollingInterest']) + "): " + str(lngIS9))
        if lngIS10:
            self.logger.debug("IS10: ComprehensiveIncome(" + str(self.fields['ComprehensiveIncome']) + ") = NetIncomeLoss(" + str(self.fields['NetIncomeLoss']) + ") , OtherComprehensiveIncome(" + str(self.fields['OtherComprehensiveIncome']) + "): " + str(lngIS10))
        if lngIS11:
            self.logger.debug("IS11: OperatingIncomeLoss(" + str(self.fields['OperatingIncomeLoss']) + ") = Revenues(" + str(self.fields['Revenues']) + ") - CostsAndExpenses(" + str(self.fields['CostsAndExpenses']) + ") , OtherOperatingIncome(" + str(self.fields['OtherOperatingIncome']) + "): " + str(lngIS11))



        ###Cash flow statement

        #NetCashFlow
        self.fields['NetCashFlow'] = self.GetFactValue("us-gaap:CashAndCashEquivalentsPeriodIncreaseDecrease", "Duration")
        if self.fields['NetCashFlow']== None:
            self.fields['NetCashFlow'] = self.GetFactValue("us-gaap:CashPeriodIncreaseDecrease", "Duration")
            if self.fields['NetCashFlow']== None:
                self.fields['NetCashFlow'] = self.GetFactValue("us-gaap:NetCashProvidedByUsedInContinuingOperations", "Duration")
                if self.fields['NetCashFlow']== None:
                    self.fields['NetCashFlow'] = 0
                
        #NetCashFlowsOperating
        self.fields['NetCashFlowsOperating'] = self.GetFactValue("us-gaap:NetCashProvidedByUsedInOperatingActivities", "Duration")
        if self.fields['NetCashFlowsOperating']== None:
            self.fields['NetCashFlowsOperating'] = 0
                
        #NetCashFlowsInvesting
        self.fields['NetCashFlowsInvesting'] = self.GetFactValue("us-gaap:NetCashProvidedByUsedInInvestingActivities", "Duration")
        if self.fields['NetCashFlowsInvesting']== None:
            self.fields['NetCashFlowsInvesting'] = 0
                
        #NetCashFlowsFinancing
        self.fields['NetCashFlowsFinancing'] = self.GetFactValue("us-gaap:NetCashProvidedByUsedInFinancingActivities", "Duration")
        if self.fields['NetCashFlowsFinancing']== None:
            self.fields['NetCashFlowsFinancing'] = 0
                
        #NetCashFlowsOperatingContinuing
        self.fields['NetCashFlowsOperatingContinuing'] = self.GetFactValue("us-gaap:NetCashProvidedByUsedInOperatingActivitiesContinuingOperations", "Duration")
        if self.fields['NetCashFlowsOperatingContinuing']== None:
            self.fields['NetCashFlowsOperatingContinuing'] = 0
                
        #NetCashFlowsInvestingContinuing
        self.fields['NetCashFlowsInvestingContinuing'] = self.GetFactValue("us-gaap:NetCashProvidedByUsedInInvestingActivitiesContinuingOperations", "Duration")
        if self.fields['NetCashFlowsInvestingContinuing']== None:
            self.fields['NetCashFlowsInvestingContinuing'] = 0
                
        #NetCashFlowsFinancingContinuing
        self.fields['NetCashFlowsFinancingContinuing'] = self.GetFactValue("us-gaap:NetCashProvidedByUsedInFinancingActivitiesContinuingOperations", "Duration")
        if self.fields['NetCashFlowsFinancingContinuing']== None:
            self.fields['NetCashFlowsFinancingContinuing'] = 0
                
        #NetCashFlowsOperatingDiscontinued
        self.fields['NetCashFlowsOperatingDiscontinued'] = self.GetFactValue("us-gaap:CashProvidedByUsedInOperatingActivitiesDiscontinuedOperations", "Duration")
        if self.fields['NetCashFlowsOperatingDiscontinued']==None:
            self.fields['NetCashFlowsOperatingDiscontinued'] = 0
                
        #NetCashFlowsInvestingDiscontinued
        self.fields['NetCashFlowsInvestingDiscontinued'] = self.GetFactValue("us-gaap:CashProvidedByUsedInInvestingActivitiesDiscontinuedOperations", "Duration")
        if self.fields['NetCashFlowsInvestingDiscontinued']== None:
            self.fields['NetCashFlowsInvestingDiscontinued'] = 0
                
        #NetCashFlowsFinancingDiscontinued
        self.fields['NetCashFlowsFinancingDiscontinued'] = self.GetFactValue("us-gaap:CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations", "Duration")
        if self.fields['NetCashFlowsFinancingDiscontinued']== None:
            self.fields['NetCashFlowsFinancingDiscontinued'] = 0
                
        #NetCashFlowsDiscontinued
        self.fields['NetCashFlowsDiscontinued'] = self.GetFactValue("us-gaap:NetCashProvidedByUsedInDiscontinuedOperations", "Duration")
        if self.fields['NetCashFlowsDiscontinued']== None:
            self.fields['NetCashFlowsDiscontinued'] = 0
                
        #ExchangeGainsLosses
        self.fields['ExchangeGainsLosses'] = self.GetFactValue("us-gaap:EffectOfExchangeRateOnCashAndCashEquivalents", "Duration")
        if self.fields['ExchangeGainsLosses']== None:
            self.fields['ExchangeGainsLosses'] = self.GetFactValue("us-gaap:EffectOfExchangeRateOnCashAndCashEquivalentsContinuingOperations", "Duration")
            if self.fields['ExchangeGainsLosses']== None:
                self.fields['ExchangeGainsLosses'] = self.GetFactValue("us-gaap:CashProvidedByUsedInFinancingActivitiesDiscontinuedOperations", "Duration")
                if self.fields['ExchangeGainsLosses']== None:
                    self.fields['ExchangeGainsLosses'] = 0

        ####Adjustments
        #Impute: total net cash flows discontinued if not reported
        if self.fields['NetCashFlowsDiscontinued']==0:
            self.fields['NetCashFlowsDiscontinued'] = self.fields['NetCashFlowsOperatingDiscontinued'] + self.fields['NetCashFlowsInvestingDiscontinued'] + self.fields['NetCashFlowsFinancingDiscontinued']

        #Impute: cash flows from continuing
        if self.fields['NetCashFlowsOperating']!=0 and self.fields['NetCashFlowsOperatingContinuing']==0:
            self.fields['NetCashFlowsOperatingContinuing'] = self.fields['NetCashFlowsOperating'] - self.fields['NetCashFlowsOperatingDiscontinued']
        if self.fields['NetCashFlowsInvesting']!=0 and self.fields['NetCashFlowsInvestingContinuing']==0:
            self.fields['NetCashFlowsInvestingContinuing'] = self.fields['NetCashFlowsInvesting'] - self.fields['NetCashFlowsInvestingDiscontinued']
        if self.fields['NetCashFlowsFinancing']!=0 and self.fields['NetCashFlowsFinancingContinuing']==0:
            self.fields['NetCashFlowsFinancingContinuing'] = self.fields['NetCashFlowsFinancing'] - self.fields['NetCashFlowsFinancingDiscontinued']
        
        
        if self.fields['NetCashFlowsOperating']==0 and self.fields['NetCashFlowsOperatingContinuing']!=0 and self.fields['NetCashFlowsOperatingDiscontinued']==0:
            self.fields['NetCashFlowsOperating'] = self.fields['NetCashFlowsOperatingContinuing']
        if self.fields['NetCashFlowsInvesting']==0 and self.fields['NetCashFlowsInvestingContinuing']!=0 and self.fields['NetCashFlowsInvestingDiscontinued']==0:
            self.fields['NetCashFlowsInvesting'] = self.fields['NetCashFlowsInvestingContinuing']
        if self.fields['NetCashFlowsFinancing']==0 and self.fields['NetCashFlowsFinancingContinuing']!=0 and self.fields['NetCashFlowsFinancingDiscontinued']==0:
            self.fields['NetCashFlowsFinancing'] = self.fields['NetCashFlowsFinancingContinuing']
        
        
        self.fields['NetCashFlowsContinuing'] = self.fields['NetCashFlowsOperatingContinuing'] + self.fields['NetCashFlowsInvestingContinuing'] + self.fields['NetCashFlowsFinancingContinuing']
        
        #Impute: if net cash flow is missing,: this tries to figure out the value by adding up the detail
        if self.fields['NetCashFlow']==0 and (self.fields['NetCashFlowsOperating']!=0 or self.fields['NetCashFlowsInvesting']!=0 or self.fields['NetCashFlowsFinancing']!=0):
            self.fields['NetCashFlow'] = self.fields['NetCashFlowsOperating'] + self.fields['NetCashFlowsInvesting'] + self.fields['NetCashFlowsFinancing']

        
        lngCF1 = self.fields['NetCashFlow'] - (self.fields['NetCashFlowsOperating'] + self.fields['NetCashFlowsInvesting'] + self.fields['NetCashFlowsFinancing'] + self.fields['ExchangeGainsLosses'])
        if lngCF1!=0 and (self.fields['NetCashFlow'] - (self.fields['NetCashFlowsOperating'] + self.fields['NetCashFlowsInvesting'] + self.fields['NetCashFlowsFinancing'] + self.fields['ExchangeGainsLosses'])==(self.fields['ExchangeGainsLosses']*-1)):       
            lngCF1 = 888888
            #What is going on here is that 171 filers compute net cash flow differently than everyone else.  
            #What I am doing is marking these by setting the value of the test to a number 888888 which would never occur naturally, so that I can differentiate this from errors.
        lngCF2 = self.fields['NetCashFlowsContinuing'] - (self.fields['NetCashFlowsOperatingContinuing'] + self.fields['NetCashFlowsInvestingContinuing'] + self.fields['NetCashFlowsFinancingContinuing'])
        lngCF3 = self.fields['NetCashFlowsDiscontinued'] - (self.fields['NetCashFlowsOperatingDiscontinued'] + self.fields['NetCashFlowsInvestingDiscontinued'] + self.fields['NetCashFlowsFinancingDiscontinued'])
        lngCF4 = self.fields['NetCashFlowsOperating'] - (self.fields['NetCashFlowsOperatingContinuing'] + self.fields['NetCashFlowsOperatingDiscontinued'])
        lngCF5 = self.fields['NetCashFlowsInvesting'] - (self.fields['NetCashFlowsInvestingContinuing'] + self.fields['NetCashFlowsInvestingDiscontinued'])
        lngCF6 = self.fields['NetCashFlowsFinancing'] - (self.fields['NetCashFlowsFinancingContinuing'] + self.fields['NetCashFlowsFinancingDiscontinued'])
        
        
        if lngCF1:
            self.logger.debug("CF1: NetCashFlow(" + str(self.fields['NetCashFlow']) + ") = (NetCashFlowsOperating(" + str(self.fields['NetCashFlowsOperating']) + ") , (NetCashFlowsInvesting(" + str(self.fields['NetCashFlowsInvesting']) + ") , (NetCashFlowsFinancing(" + str(self.fields['NetCashFlowsFinancing']) + ") , ExchangeGainsLosses(" + str(self.fields['ExchangeGainsLosses']) + "): " + str(lngCF1))
        if lngCF2:
            self.logger.debug("CF2: NetCashFlowsContinuing(" + str(self.fields['NetCashFlowsContinuing']) + ") = NetCashFlowsOperatingContinuing(" + str(self.fields['NetCashFlowsOperatingContinuing']) + ") , NetCashFlowsInvestingContinuing(" + str(self.fields['NetCashFlowsInvestingContinuing']) + ") , NetCashFlowsFinancingContinuing(" + str(self.fields['NetCashFlowsFinancingContinuing']) + "): " + str(lngCF2))
        if lngCF3:
            self.logger.debug("CF3: NetCashFlowsDiscontinued(" + str(self.fields['NetCashFlowsDiscontinued']) + ") = NetCashFlowsOperatingDiscontinued(" + str(self.fields['NetCashFlowsOperatingDiscontinued']) + ") , NetCashFlowsInvestingDiscontinued(" + str(self.fields['NetCashFlowsInvestingDiscontinued']) + ") , NetCashFlowsFinancingDiscontinued(" + str(self.fields['NetCashFlowsFinancingDiscontinued']) + "): " + str(lngCF3))
        if lngCF4:
            self.logger.debug("CF4: NetCashFlowsOperating(" + str(self.fields['NetCashFlowsOperating']) + ") = NetCashFlowsOperatingContinuing(" + str(self.fields['NetCashFlowsOperatingContinuing']) + ") , NetCashFlowsOperatingDiscontinued(" + str(self.fields['NetCashFlowsOperatingDiscontinued']) + "): " + str(lngCF4))
        if lngCF5:
            self.logger.debug("CF5: NetCashFlowsInvesting(" + str(self.fields['NetCashFlowsInvesting']) + ") = NetCashFlowsInvestingContinuing(" + str(self.fields['NetCashFlowsInvestingContinuing']) + ") , NetCashFlowsInvestingDiscontinued(" + str(self.fields['NetCashFlowsInvestingDiscontinued']) + "): " + str(lngCF5))
        if lngCF6:
            self.logger.debug("CF6: NetCashFlowsFinancing(" + str(self.fields['NetCashFlowsFinancing']) + ") = NetCashFlowsFinancingContinuing(" + str(self.fields['NetCashFlowsFinancingContinuing']) + ") , NetCashFlowsFinancingDiscontinued(" + str(self.fields['NetCashFlowsFinancingDiscontinued']) + "): " + str(lngCF6))


        #Key ratios
        try:
            self.fields['SGR'] = ((self.fields['NetIncomeLoss'] / self.fields['Revenues']) * (1 + ((self.fields['Assets'] - self.fields['Equity']) / self.fields['Equity']))) / ((1 / (self.fields['Revenues'] / self.fields['Assets'])) - (((self.fields['NetIncomeLoss'] / self.fields['Revenues']) * (1 + (((self.fields['Assets'] - self.fields['Equity']) / self.fields['Equity']))))))
        except:
            pass
            
        try:
            self.fields['ROA'] = self.fields['NetIncomeLoss'] / self.fields['Assets']
        except:
            pass
        
        try:    
            self.fields['ROE'] = self.fields['NetIncomeLoss'] / self.fields['Equity']
        except:
            pass
            
        try:    
            self.fields['ROS'] = self.fields['NetIncomeLoss'] / self.fields['Revenues']
        except:
            pass           
