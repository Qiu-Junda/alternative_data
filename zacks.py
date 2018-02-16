import os
import logging
import pandas as pd
from bs4 import BeautifulSoup
from urllib.request import urlopen


class Zacks:
    def __init__(self):
        self.logger = logging.getLogger('zacks')
        self.url_head = 'https://www.zacks.com/stock/research/'
        self.url_tail = '/brokerage-recommendations'
        self.tags_to_parse = [12, 14, 15, 17, 18]

    def download(self, tickers):
        recommendation_details = pd.DataFrame(columns=tickers)
        for ticker in tickers:
            r = urlopen(self.url_head + ticker + self.url_tail)
            soup = BeautifulSoup(r, 'html.parser')
            trTags = soup.find_all('tr')

            for idx in self.tags_to_parse:
                if idx == 12:

                    # 12: Average Broker Recommendation (ABR)
                    for i, child in enumerate(trTags[idx].children):
                        if i == 1:
                            row_idx = child.contents[0]
                        elif i == 3:
                            recommendation_details.loc[row_idx, ticker] = child.string

                else:

                    # 14: Number of recommendations
                    # 15: Average Target Price
                    # 17: Industry
                    # 18: Industry rank
                    for i, child in enumerate(trTags[idx].children):
                        if i == 1:
                            row_idx = child.string
                        elif i == 3:
                            recommendation_details.loc[row_idx, ticker] = child.string

        return recommendation_details
