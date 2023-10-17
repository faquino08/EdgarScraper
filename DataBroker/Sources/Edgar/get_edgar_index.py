import edgar
import os
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def get_edgar_index(year=2022,endYear=None):
    '''
    Get index of EDGAR filings going back to beginning of provided year.
    year -> (int) year to start at
    '''
    user_agent = "FTC edward@ftc.com"
    edgar.download_index("./index", year, user_agent, skip_all_present_except_last=False)

    #Linux
    logger.info(endYear)
    if endYear is not None:
        yearsToGet = range(year,endYear+1,1)
        logger.info('Years To Get: ' + str(yearsToGet))
        files = os.listdir("./index/")
        for file in files:
            if int(file[:4]) not in yearsToGet:
                os.system('rm -rf ./index/%s' % str(file))
    os.system('cat ./index/*.tsv > ./index/master.csv')
    os.system('rm -rf ./index/*.tsv')