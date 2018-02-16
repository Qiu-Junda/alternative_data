import sec
import time
import logging
import datetime
import fundamentals
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen


# Not finished yet
class Filings(fundamentals.Fundamentals):
    def __init__(self):
        super(Filings, self).__init__()

        # Setup logger
        self._logger = logging.getLogger('filings')

        # Define text rqd for database creation and maintenance
        self.tbl_name = 'filings'
        self._sec_date_fmt = '%Y-%m-%d'
        self._sec_before_date_fmt = '%Y%m%d'

        # Note price, high price and low price has been ommitted
        self._sql_cols = [
            'Ticker', 'FilingDate', 'QuarterEnd', 'Shares', 'SharesSplitAdjusted', 'SplitFactor', 'Assets',
            'CurrentAssets', 'Liabilities', 'CurrentLiabilities', 'ShareholdersEquity', 'NonControllingInt',
            'PreferredEquity', 'GoodwillAndIntangibles', 'LongTermDebt', 'Revenue', 'Earnings',
            'EarningsAvailableForCommonStockholders', 'EPS basic', 'EpsDiluted', 'DividendPerShare', 'OperatingCash',
            'InvestingCash', 'FinancingCash', 'CashChg', 'NetCash', 'Capex', 'ROE', 'ROA', 'BookValue',
            'PriceBookRatio', 'PriceEquity', 'CumDividendsPerShare', 'DividendPayoutRatio',
            'LongTermDebtToEquityRatio', 'EquityToAssetsRatio', 'NetMargin', 'AssetTurnOver',
            'FreeCashFlowPerShare', 'CurrentRatio'
        ]
        self._sql_col_dtypes = ['TEXT NOT NULL', 'DATE NOT NULL', 'DATE NOT NULL', 'INTEGER', 'INTEGER'] + \
                               ['REAL'] * (len(self._sql_cols) - 4)
        self._create_tbl_sql = 'CREATE TABLE IF NOT EXISTS ' + self.tbl_name + '(' + \
                               ', '.join([' '.join([col_name, col_dtype]) for col_name, col_dtype in
                                          zip(self._sql_cols, self._sql_col_dtypes)]) + ')'
        self._get_last_parsed_date_sql = ' '.join([
            'SELECT FilingDate FROM', self.tbl_name, 'WHERE ticker=? ORDER BY FilingDate DESC Limit 1'
        ])
        self._insert_new_row_of_data_sql = ' '.join([
            'INSERT INTO', self.tbl_name, 'VALUES', '(' + '?' * len(self._sql_cols) + ')'
        ])

        # Define rqd utils for data parsing
        self.url = 'http://www.stockpup.com/data/'

        # Creating the database if non existent
        self.cursor.execute(self._create_tbl_sql)

    def _yield_records(self):
        r = urlopen(self.url).read()
        soup = BeautifulSoup(r, 'html.parser')
        links = [link.get('href') for link in soup.find_all('a')]
        for link in links:
            if isinstance(link, str) and 'quarterly_financial_data.csv' in link:
                file = link.split('/')[2]
                ticker = file.split('_')[0]

                self._logger.info('Yielding data for %s...' % ticker)
                yield ticker, pd.read_csv(
                    self.url + file, parse_dates=[0], dayfirst=True
                ).sort_values(by='Quarter end').reset_index(drop=True)

    def update(self):

        # Initializing downloader class to download filings date from SEC
        sec_engine = sec.SEC()

        # Iterating over the consolidated filings on stockpup website
        for ticker, records_to_store in self._yield_records():

            # Recording time at which we started on one file
            start_time = time.time()

            rearranged_records_to_store = self._convert_to_sql_fmt(ticker, records_to_store)
            self.cursor.execute(self._get_last_parsed_date_sql, (ticker, ))
            last_parsed_date = self.cursor.fetchall()

            # Although the filings will be first shown in SEC rather than from this guy's website, we depend on
            # him to clean up the data for us. Thus, there is no point in checking SEC for any new filings. We might
            # as well just check his website if there are any updated filings he has consolidated for us - and when
            # there is, we will then check SEC for the filing date for that new filing.
            if not last_parsed_date:
                self._logger.info('No existing filings data for %s' % ticker)
                records_to_find_date_for = rearranged_records_to_store.copy()
            else:
                self._logger.info('Last filing date for %s is %s' % (ticker, last_parsed_date))
                records_to_find_date_for = rearranged_records_to_store.loc[
                                            rearranged_records_to_store['QuarterEnd'] > last_parsed_date, :
                                           ]

            n_records = len(records_to_find_date_for)
            for idx in range(n_records):
                if idx < n_records:

                    # This works on the logic that they must have filed their previous quarter statements
                    # before the next quarter, so the next quarter's date can be used a proxy for the latest
                    # date for which they must have filed the statements.
                    next_quarter_date = records_to_find_date_for.loc[
                        records_to_find_date_for.index[idx + 1], 'QuarterEnd'
                    ]
                else:

                    # On their most latest filing, the fact of it being their latest filing means there won't
                    # be another next_quarter_date available. In that case, the best proxy is to use today's date.
                    next_quarter_date = datetime.datetime.today().date().strftime(sec_engine.before_date_fmt)

                # Note the below operations work on the assumption that the data is arranged ascending order
                row = records_to_find_date_for.loc[idx, :]

                row.loc['FilingDate'] = sec_engine.update(
                    doc_type='10-', before_date=next_quarter_date.strftime('%Y%m%d'), ciks={ticker: ticker}
                )
                self.cursor.execute(self._insert_new_row_of_data_sql, tuple(row.values))

            # Measuring time taken
            time_elapsed = time.time() - start_time
            if time_elapsed < 5:
                self._logger.warning('Only %f seconds has elapsed since last parse. Sleeping %f '
                                     'seconds more to allow continuous parsing...' % (time_elapsed, 5 - time_elapsed))
                time.sleep(round(5 - time_elapsed))

    def _convert_to_sql_fmt(self, ticker, records_to_store):

        # Dropping unnecessary data
        records_to_store.drop(axis=1, labels=['Price', 'Price high', 'Price low'], inplace=True)

        # Changing column names
        existing_col_names = records_to_store.columns.tolist()
        records_to_store.rename(
            inplace=True,
            columns={
                existing_col_name: new_col_name for existing_col_name, new_col_name in
                zip(existing_col_names, self._sql_cols[2:])
            }
        )

        records_to_store['Ticker'] = ticker
        records_to_store['FilingDate'] = None  # Filling in the column first
        records_to_store = records_to_store[self._sql_cols]

        self._logger.info('Successfully converted %s file into SQL format...' % ticker)
        return records_to_store


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db = Filings()
    db.update()
    db.conn.commit()
    db.conn.close()
