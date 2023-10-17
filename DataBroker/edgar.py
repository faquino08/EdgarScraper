from DataBroker.Sources.Edgar.secFunctions import secFunctions

def getFyAndFq(params,debug,cik):
    '''
    Wrapper function for secFunctions.getFyAndFqReports().
    debug -> (boolean) Whether to record debug logs
    cik -> (str) Central Index Key from SEC
    '''
    secApi = secFunctions(postgresParams=params,debug=debug)
    secApi.getFyAndFqReports(cik,False,True)
    return

def getFyAndFqList(params,debug,ciks):
    '''
    Wrapper function for secFunctions.getFyAndFqReports().
    debug -> (boolean) Whether to record debug logs
    cik -> (str) Central Index Key from SEC
    '''
    secApi = secFunctions(postgresParams=params,debug=debug)
    secApi.getFyAndFqReportsList(ciks,False,True)
    return

def getMissingTickers(params,debug,date):
    '''
    Wrapper function for secFunctions.getTickersNotInDb().
    debug -> (boolean) Whether to record debug logs
    date -> (str) "YYYY-MM-dd" Earliest date of filings to parse in \
                edgarindex
    '''
    secApi = secFunctions(postgresParams=params,debug=debug)
    secApi.getTickersNotInDb(date=date)
    return

def getMissingFilingsIndex(params,debug,year,endYear=None):
    '''
    Wrapper function for secFunctions.getIndexEntriesOfFilingsMissing().
    debug -> (boolean) Whether to record debug logs
    year -> (int) cut off year to look for missing filing entries
    '''
    secApi = secFunctions(postgresParams=params,debug=debug)
    secApi.getIndexEntriesOfFilingsMissing(year,endYear)
    return