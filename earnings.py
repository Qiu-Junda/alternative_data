import logging
import calendar
import datetime
import fundamentals
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen


class Earnings(fundamentals.Fundamentals):
    """
    This class is used to parse earnings release dates from NASDAQ's website. Primarily this is used for me to
    trade earnings season since I like trading event-driven strategies with equity options during earnings.
    """
    def __init__(self):
        super(Earnings, self).__init__()

        # Define text rqd for database creation and maintenance
        self.tbl_name = 'earnings'
        self._nasdaq_date_fmt = '%d%m%Y'

        self._sql_cols = ['date', 'ticker', 'quarter', 'forecast', 'estimates', 'actual', 'surprise']
        self._sql_col_dtypes = ['DATE NOT NULL', 'TEXT NOT NULL', 'TEXT', 'REAL', 'INTEGER NOT NULL',
                                'REAL NOT NULL', 'REAL']
        self._create_tbl_sql = 'CREATE TABLE IF NOT EXISTS ' + self.tbl_name + '(' + \
                               ', '.join([' '.join(
                                   [col_name, col_dtype]
                               ) for col_name, col_dtype in zip(self._sql_cols, self._sql_col_dtypes)]) + ')'
        self._get_last_date_sql = 'SELECT date FROM ' + self.tbl_name + ' ORDER BY ROWID DESC limit 1'
        self._get_earnings_sql = 'SELECT * FROM ' + self.tbl_name + ' WHERE ticker=? ORDER BY date'

        # Define rqd utils for data parsing
        self._earliestDate = datetime.date(year=2013, month=1, day=3)
        self.url = 'http://www.nasdaq.com/earnings/earnings-calendar.aspx?date='
        self._original_cols = ['Ticker', 'Fiscal Quarter Ending', 'Forecast', '# of Ests', 'Actual', '% Surprise']

        # Setup logger
        self._logger = logging.getLogger(self.tbl_name)

        # Creating the database if non existent
        self.cursor.execute(self._create_tbl_sql)

    def _find_last_date(self):
        self.cursor.execute(self._get_last_date_sql)
        dates = self.cursor.fetchone()
        if not dates:
            self._logger.info('Using nasdaq\'s earliest date since this is first time database is initialized')
            return self._earliestDate
        else:
            self._logger.info('Last parsed date is %s so restarting from there' %
                              (dates[0].strftime(self._nasdaq_date_fmt)))
            return dates[0]

    def update(self):

        # Get latest date for which we need to parse data
        nxt_avail_date = self._find_last_date()
        ytd_date = datetime.datetime.today().date() - datetime.timedelta(days=1)

        # Parse data continuously via a loop
        while nxt_avail_date < ytd_date:
            nxt_avail_date += datetime.timedelta(days=1)
            self._update(nxt_avail_date)
        self._logger.info('Earnings date updates are complete.')

    def _update(self, nxt_avail_date):
        parsed_data = pd.DataFrame()

        # Structuring the url based on given date
        url = self.url + '-'.join([str(nxt_avail_date.year),
                                   calendar.month_abbr[nxt_avail_date.month],
                                   str(nxt_avail_date.day)])

        # Parsing the data via BeautifulSoup
        r = urlopen(url, timeout=10).read()
        soup = BeautifulSoup(r, 'html.parser')
        tr_tags = soup.find_all('tr')
        relevant_tr_tags = tr_tags[4:]  # The first 3 tr tags are always irrelevant to us so ignore them

        # Parsing the information we need that is stored inside each tr tag
        for relevant_tr_tag in relevant_tr_tags:
            collected_data = []  # Parsed data are stored here

            for child in relevant_tr_tag.children:
                try:
                    child_contents = child.contents
                except AttributeError:  # some tags have children with no navigable strings
                    pass
                else:
                    if len(child_contents) == 1:  # 2nd to 2nd last tag is parsed here
                        for string in child.stripped_strings:
                            collected_data.append(string)
                    elif len(child_contents) == 3:  # 1st and last tag is parsed here
                        sub_child_contents = child_contents[1].contents[0]
                        if '(' in sub_child_contents:
                            ticker = sub_child_contents.split('(')[-1].split(')')[0]
                            collected_data.append(ticker)
                        else:
                            collected_data.append(sub_child_contents)
                    else:

                        # Since 1st Feb 2018, Nasdaq has implemented a new format in its html code. The prior
                        # condition which I used to test if data has ended is initially the if paragraph below
                        # where I test for "No data available" no longer works. Instead the existence of a new
                        # table cause my code to jump to here immediately. Since none of the previous parsing
                        # has reached this point yet, I will use this segment to mirror the initial exit.
                        self._logger.warning(
                            'New format which started on 1st Feb 2018 encountered for %s. Mirroring initial exit.' %
                            nxt_avail_date.strftime(self._nasdaq_date_fmt))
                        collected_data.append('No data available')
                        break

            if 'No data available' in collected_data[0]:  # checker to ensure we don't parse unnecessary data
                self._logger.info('All data parsed for %s. Moving on...' % nxt_avail_date.strftime(self._nasdaq_date_fmt))

                # When parsing the desired data, the entire loop should end in this paragraph
                if not parsed_data.empty:
                    transformed_data = self._convert_to_sql_fmt(nxt_avail_date, parsed_data)
                    self.store(transformed_data)
                    return
                break
            else:
                if len(collected_data) != 8:  # Error checking to ensure parsed data is of right length
                    self._logger.warning('Parsed data for %s seems to have missing or extra values.' %
                                         nxt_avail_date.strftime(self._nasdaq_date_fmt))

                    # The first time I encountered an exception was for a ticker LPL on 22nd Jan 2018. The HTML code
                    # had a image file that was causing me problems, so for quick fix I did the following. This
                    # allowed me to take away any extra elements at the front of the list.
                    while collected_data[0] != ticker:
                        collected_data = collected_data[1:]

                del collected_data[-3]

                df = pd.DataFrame(index=self._original_cols,
                                  columns=[collected_data[1]],
                                  data=collected_data[:1] + collected_data[2:]).T
                self._logger.info('Data for %s parsed...' % ticker)

                if not parsed_data.empty:
                    parsed_data = pd.concat([parsed_data, df], axis=0, join='outer')
                else:
                    parsed_data = df

    def _convert_to_sql_fmt(self, nxt_avail_date, parsed_data):
        parsed_data = parsed_data.rename(
            columns={original: new for original, new in zip(self._original_cols, self._sql_cols[1:])}
        )  # Rename columns to desired names as in sql table
        parsed_data.index = [nxt_avail_date] * len(parsed_data)  # Change index from date strings to datetime.date types
        return parsed_data

    def store(self, records_to_store):
        if isinstance(records_to_store, pd.DataFrame):
            records_to_store.to_sql(self.tbl_name, self.conn, if_exists='append', index=True, index_label='date')
        else:
            raise NotImplementedError('Have not accounted for appending other kinds of data structures')

    def traverse(self, ticker):
        self.cursor.execute(self._get_earnings_sql, ticker)
        yield from self.cursor.fetchall()


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    db = Earnings()
    db.update()
    db.conn.commit()
    db.conn.close()
