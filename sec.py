import re
import pprint
import logging
import calendar
import datetime
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen


class SEC:
    """
    This class is a parser which parses data from the SEC Edgar filings site. It is used primarily for
    1) Replicating 13F portfolios from investment gurus
    2) Finding dates for filings by companies

    In particular to point 2, it is used in conjunction with my Filings class. See comments in my Filings class
    to understand the rationale of a separate class for SEC filings.
    """
    def __init__(self):

        # Setup logger
        self.logger = logging.getLogger('sec')

        # Define different parts of url for merging when parsing data
        self._url_head = 'https://www.sec.gov'
        self._url_mid = '/cgi-bin/browse-edgar?action=getcompany'
        self._cik = '&CIK='
        self._doc_type = '&type='
        self._before_date = '&dateb='
        self._ownership = '&owner=exclude'
        self._hide_filings = '&hidefilings=0'
        self._doc_count = '&count='
        self._doc_start_idx = '&start='
        self._max_doc_to_show = '100'

        # Define details for 13F which we want to replicate
        self.gurus_to_replicate = {'Greenblatt': '0001510387', 'Munger': '0000783412', 'Simpson': '0001534380',
                                   'Yacktman': '0000905567', 'Ackman': '0001336528', 'Buffet': '0001067983',
                                   'Akre': '0001112520'}
        self.top_rank_to_use = {'Greenblatt': 10, 'Munger': 5, 'Simpson': 10, 'Yacktman': 10,
                                'Ackman': 3, 'Buffet': 12, 'Akre': 10}

        # Define utils to ease parsing of data
        self.before_date_fmt = '%Y%m%d'
        self.data_store_for_13F = None

    def _reset_data_store(self):
        self.data_store_for_13F = {'date': [], 'security': [], 'cusip': [], 'title': [], 'qty': [], 'value': [],
                                   'opt_type': [], 'sec_type': [], 'voting_auth': []}

    def update(self, doc_type, before_date='', ciks=None):

        # Error checking to ensure correct combination of doc_type and ciks
        if doc_type == '13F-HR':
            if ciks is not None:
                raise ValueError('Using a document type of 13F-HR only makes sense for gurus data, and ' +
                                 'not the CIKs you have chosen. Please ignore the CIKs argument if you ' +
                                 'wish to parse only gurus data.')
            else:
                ciks_to_use = self.gurus_to_replicate
        else:
            if ciks is None:
                raise ValueError('Need CIKs to know which companies to parse 10-K/10-Q for.')
            else:
                ciks_to_use = ciks

        # Error checking to ensure correct format of before_date was given
        if isinstance(before_date, str):
            if before_date != '':
                try:
                    _ = datetime.datetime.strptime(before_date, self.before_date_fmt)
                except ValueError as e:
                    self.logger.error('Your date was not provided in the format %s' % self.before_date_fmt)
                    exit()
            else:
                self.logger.info('Parsing the latest file...')
        else:
            raise TypeError('before_date should be a string with format %s.' % self.before_date_fmt)

        # Looping over ciks to parse data for
        for name, cik in ciks_to_use.items():
            self.logger.info('Parsing data for ' + name + '...')
            self._reset_data_store()

            filing_date, holdings = self._update(doc_type, before_date, cik)

            if holdings is None:
                if doc_type == '13F-HR':
                    self.logger.info('Ignoring documents for ' + name + ' filed on ' + filing_date + '...')
                    continue
                else:
                    raise SyntaxError('Holdings returned cannot be none for 10-K/10-Q.')
            else:
                if doc_type == '13F-HR':
                    sub_hldgs = holdings.iloc[:self.top_rank_to_use[name], :]
                    sub_hldgs.loc[:, 'PctHldg'] = sub_hldgs.loc[:, 'value'] / sub_hldgs.sum()['value']
                    sub_hldgs = sub_hldgs.sort_values(by='PctHldg', ascending=False)
                    print(name, cik, filing_date)
                    pprint.pprint(sub_hldgs)
                else:
                    return filing_date

    @staticmethod
    def _get_last_ended_qtr_date():
        today = datetime.datetime.today()
        qtr = ((today.month - 1) // 3)
        qtr_mth = 12 if qtr == 0 else qtr * 3
        qtr_year = today.year - 1 if qtr_mth == 12 else today.year
        qtr_last_day = calendar.monthrange(qtr_year, qtr_mth)[1]
        return datetime.date(year=qtr_year, month=qtr_mth, day=qtr_last_day)

    def _update(self, doc_type, before_date, cik):

        # Determining URL based on document type since URL structure for 13F vs 10-K/10-Q is different
        if doc_type == '13F-HR':
            url = self._url_head + self._url_mid + self._cik + cik + self._doc_type + doc_type + \
                  self._before_date + before_date + self._ownership + self._doc_count + self._hide_filings
        else:
            url = self._url_head + self._url_mid + self._cik + cik + self._doc_type + doc_type + \
                  self._before_date + before_date + self._ownership + self._doc_count + self._max_doc_to_show

        # Opening the site and parsing the XML data
        soup = BeautifulSoup(urlopen(url), 'html.parser')
        tr_tags = soup.find_all('tr')
        for tr_tag in tr_tags:
            tr_tag_contents = tr_tag.contents
            for tr_tag_content in tr_tag_contents:
                print(tr_tag_content)
                if tr_tag_content.string == doc_type:
                    if doc_type == '13F-HR':
                        documents_link = self._url_head + tr_tag_contents[3].a.get('href')
                    elif '/' not in doc_type:
                        a_tags = tr_tag_contents[3].find_all('a')
                        documents_link = self._url_head + a_tags[1].get('href')
                    else:
                        self.logger.warning('Document type %s detected in SEC website.')
                        continue

                    # Reformatting filing date to a datetime format as it is encoded in UTF-8
                    filing_date = tr_tag_contents[7].string
                    filing_date_str = str(filing_date.encode('utf-8'))[2:12]
                    reformatted_filing_date = datetime.datetime.strptime(filing_date_str, '%Y-%m-%d').date()

                    # If latest filing date is smaller than last ended quarter date, we probably already acted
                    # on this information previously. The latest filing is always after the last ended quarter date.
                    if reformatted_filing_date < self._get_last_ended_qtr_date():
                        return filing_date_str, None

                    if doc_type == '13F-HR':
                        return filing_date_str, self._parse_13F(documents_link)
                    else:
                        return filing_date_str, self._parse_10K10Q(documents_link)

    def _parse_10K10Q(self, link):
        soup = BeautifulSoup(urlopen(link), 'html.parser')
        for a_tag in soup.find_all('a'):
            linkInTag = a_tag.get('href')
            if '.xlsx' in linkInTag:
                return pd.read_excel(self._url_head + linkInTag, sheetname=None)

    def _parse_13F(self, link):
        soup = BeautifulSoup(urlopen(link), 'html.parser')
        for a_tag in soup.find_all('a'):
            linkInTag = a_tag.get('href')
            if '.txt' in linkInTag:
                contents = urlopen(self._url_head + linkInTag).read().decode('utf-8')
                report_date = contents.split('PERIOD OF REPORT:\t', 1)[1].split('\n')[0]
                new_lines = [line.replace('&amp;', '&') for line in contents.split('\n')]
                parsed_data = self._parse_new_fmt(report_date, new_lines)

                # This part where the old format is used has not been error-checked. Need to find a file with
                # the old format in order to check this.
                if parsed_data.empty:
                    print('This txt file follows the old format. Parsing with old format function instead...')
                    old_fmt_data = self._parse_old_fmt(new_lines)
                    for i in range(0, len(old_fmt_data) // 6):
                        self.data_store_for_13F['security'].append(old_fmt_data[6 * i])
                        self.data_store_for_13F['sec_type'].append(old_fmt_data[6 * i + 1])
                        self.data_store_for_13F['cusip'].append(old_fmt_data[6 * i + 2])
                        self.data_store_for_13F['value'].append(old_fmt_data[6 * i + 3])
                        self.data_store_for_13F['qty'].append(old_fmt_data[6 * i + 4])
                        self.data_store_for_13F['voting_auth'].append(old_fmt_data[6 * i + 5])

                        self.data_store_for_13F['value'][-1] = int(
                            self.data_store_for_13F['value'][-1].replace(',', '')
                        ) * 1000
                        self.data_store_for_13F['qty'][-1] = int(
                            self.data_store_for_13F['qty'][-1].replace(',', '')
                        )

                    parsed_data['stock'] = self.data_store_for_13F['security']
                    parsed_data['cusip'] = self.data_store_for_13F['cusip']
                    parsed_data['mkt_val'] = self.data_store_for_13F['value']
                    parsed_data['qty'] = self.data_store_for_13F['qty']

                unique_data = parsed_data.drop_duplicates()
                agg_parsed_data = unique_data.groupby('cusip')['value'].sum().reset_index()
                agg_parsed_data = agg_parsed_data.sort_values(by='value',
                                                              ascending=False,
                                                              inplace=False).reset_index(drop=True)
                agg_parsed_data['security'] = [
                    unique_data.loc[
                        unique_data['cusip'] == cusip,
                        'security'
                    ].values[0] for cusip in agg_parsed_data['cusip'].values
                ]
                return agg_parsed_data

    def _parse_new_fmt(self, report_date, lines):
        line_num, stk_idx = 0, 0

        while line_num < len(lines):
            if '<infoTable>' in lines[line_num]:
                tbl_line_num = line_num + 1
                while '</infoTable>' not in lines[tbl_line_num]:
                    if '<nameOfIssuer>' in lines[tbl_line_num]:
                        val1 = lines[tbl_line_num].split('<nameOfIssuer>', 1)[1].split('</nameOfIssuer>')[0]
                        self.data_store_for_13F['security'].append(val1)
                        self.data_store_for_13F['date'].append(report_date)
                        self.data_store_for_13F['opt_type'].append('na')
                        stk_idx += 1

                    if '<titleOfClass>' in lines[tbl_line_num]:
                        val1 = lines[tbl_line_num].split('<titleOfClass>', 1)[1].split('</titleOfClass>')[0]
                        self.data_store_for_13F['title'].append(val1)

                    if '<cusip>' in lines[tbl_line_num]:
                        val1 = lines[tbl_line_num].split('<cusip>', 1)[1].split('</cusip>')[0]
                        self.data_store_for_13F['cusip'].append(val1)

                    if '<value>' in lines[tbl_line_num]:
                        val1 = lines[tbl_line_num].split('<value>', 1)[1].split('</value>')[0]
                        self.data_store_for_13F['value'].append(val1)

                    if '<sshPrnamt>' in lines[tbl_line_num]:
                        val1 = lines[tbl_line_num].split('<sshPrnamt>', 1)[1].split('</sshPrnamt>')[0]
                        self.data_store_for_13F['qty'].append(val1)

                    if '<putCall>' in lines[tbl_line_num]:
                        val1 = lines[tbl_line_num].split('<putCall>', 1)[1].split('</putCall>')[0]
                        self.data_store_for_13F['security'][stk_idx - 1] += ' ' + val1
                        self.data_store_for_13F['opt_type'][stk_idx - 1] = val1

                    tbl_line_num += 1
                line_num = tbl_line_num
            line_num += 1

        self.data_store_for_13F['value'] = [
                int(val.replace(',', '')) * 1000 for val in self.data_store_for_13F['value']
        ]
        self.data_store_for_13F['sec_type'] = [np.nan] * len(self.data_store_for_13F['value'])
        self.data_store_for_13F['voting_auth'] = [np.nan] * len(self.data_store_for_13F['value'])
        return pd.DataFrame.from_dict(self.data_store_for_13F)

    @staticmethod
    def _parse_old_fmt(lines):
        line_num, list_reg_vals = 0, []
        while line_num < len(lines):
            tbl_line_num = line_num
            if '<TABLE>' in lines[line_num]:
                while '-----' not in lines[line_num]:
                    line_num += 1
                    tbl_line_num = line_num

                while '</TABLE>' not in lines[tbl_line_num]:
                    tbl_line_num += 1
                    regex_val = re.split(r'(\t)', lines[tbl_line_num])
                    for p in range(0, len(regex_val)):
                        if regex_val[p] not in ['', '\t', ' ', 'x'] and ('---' not in regex_val[p]):
                            list_reg_vals.append(regex_val[p].strip())
                    line_num = tbl_line_num
            line_num += 1
        return list_reg_vals


def check_guru_portfolios():
    db = SEC()
    db.update('13F-HR')


def get_companies_filings_date():
    db = SEC()

    before_date = '20000430'
    doc_type = '10-'
    ciks = {'A': 'A'}

    db.update(doc_type, before_date, ciks)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    # check_guru_portfolios()
    get_companies_filings_date()
