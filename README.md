# EdgarSvc

Flask app that exposes an api for running workflows that gets data from the SEC's EDGAR website. It can extract 20+ pieces of financial data including Assets, Liabilities, Revenues, Gross Profit etc

[Main Repo](https://github.com/faquino08/FinanceDb/blob/main/README.md)

# Docker Reference

The following is a description of each env var key and value:

**Key Name:** POSTGRES_DB \
**Description:** a string containing the name of the postgres database for data insertion. \
**Values:** <span style="color:#6C8EEF">\<postgres database name string></span>

**Key Name:** POSTGRES_USER \
**Description:**  a string containing the username the postgres server to use for authentication. \
**Values:** <span style="color:#6C8EEF">\<postgres username string></span>

**Key Name:** POSTGRES_PASSWORD \
**Description:** a string containing the password the postgres user specified. \
**Values:** <span style="color:#6C8EEF">\<postgres password string></span>

**Key Name:** POSTGRES_LOCATION \
**Description:** a string containing the hostname for the postgres server. \
**Values:** <span style="color:#6C8EEF">\<postgres hostname string></span>

**Key Name:** POSTGRES_PORT \
**Description:** a string containing the port for the postgres server. \
**Values:** <span style="color:#6C8EEF">\<postgres port string></span>

**Key Name:** DEBUG_BOOL \
**Description:** a string determining whether logging should include debug level messages. \
**Values:** <span style="color:#6C8EEF">True|False</span>

# Api Reference

[comment]: <> (First Command)
### <span style="color:#6C8EEF">**POST**</span> /manual_edgar_missing_entries?year=<span style="color:#a29bfe">**:int**</span>&end=<span style="color:#a29bfe">**:int**</span>
Gets the EDGAR index of all filings from `year` to `end`.

#### **Arguments:**
- **year** - start year of the EDGAR index to save.
- **end** - end year of the EDGAR index to save.

[comment]: <> (Second Command)
### <span style="color:#6C8EEF">**POST**</span> /manual_edgar_missing_tickers?date=<span style="color:#a29bfe">**:int**</span>
Goes through the EDGAR 10K and 10Q filings going back to `date` and retrieves their tickers to connect them to their CIK.

#### **Arguments:**
- **date** - string of the earliest date to go back to for identifying tickers. *Default:* ***2020-01-01***

[comment]: <> (Third Command)
### <span style="color:#6C8EEF">**POST**</span> /manual_edgar_getfy_fq?cik=<span style="color:#a29bfe">**:int**</span>&delay=<span style="color:#a29bfe">**:int**</span>
Gets all available FQ and FY filings available for a particular CIK. Extracts the data from the xbrl filings and inserts them into the Postgres database.

#### **Arguments:**
- **cik** - the Central Index Key to lookup. *Default:* ***1390777***
- **delay** - integer showing how many seconds before starting the workflow. *Default:* ***30***

[comment]: <> (Fourth Command)
### <span style="color:#6C8EEF">**POST**</span> /manual_edgar_getfy_fq_list?ciks=<span style="color:#a29bfe">**:list**</span>&delay=<span style="color:#a29bfe">**:int**</span>
Gets all available FQ and FY filings available for a list of CIKs. Extracts the data from the xbrl filings and inserts them into the Postgres database.

#### **Arguments:**
- **ciks** - list of Central Index Keys to lookup. *Default:* ***[]***
- **delay** - integer showing how many seconds before starting the workflow. *Default:* ***30***
