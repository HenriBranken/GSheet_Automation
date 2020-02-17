#!/usr/bin/env python3

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import logging
import datetime
import dateutil.parser
import argparse
import requests
import json

log = logging.getLogger(__name__)

num_alph_mapper = {
    1: "B",
    2: "C",
    3: "D",
    4: "E",
    5: "F"
}


def extract_dictionaries(tickers, master_data):
    dictionary_list = []
    symbols_present = []
    for entry in master_data:
        if entry["symbol"] in tickers:
            dictionary_list.append(entry)
            symbols_present.append(entry["symbol"])
    return dictionary_list, symbols_present


def infer_datetime(symbol_string):
    yymmdd = symbol_string[-6:]
    yyyy = "20" + yymmdd[0:2]
    mm = yymmdd[2:4]
    dd = yymmdd[4:6]
    expiry_dt = datetime.date(year=int(yyyy), month=int(mm), day=int(dd))
    return expiry_dt


class GsheetUpdater:
    def __init__(self, credentials, price_definition, sleep_interval, api_url, ticker_roots,
                 perpetual_name, workbook_name, wks_num):
        log.info("Initializing GSheet_updater...")
        self.scope = ['https://www.googleapis.com/auth/drive']
        self.credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials, self.scope)
        self.gc = gspread.authorize(self.credentials)
        self.sh = self.gc.open(workbook_name)
        self.wks1 = self.sh.get_worksheet(wks_num)
        self.price_definition = price_definition
        self.sleep_interval = sleep_interval
        self.api_url = api_url
        self.ticker_roots = ticker_roots
        self.data_dict = dict()
        self.perp = perpetual_name

    @staticmethod
    def produce_yy_strings():
        yy_int = datetime.datetime.now().year
        yy_next_int = yy_int + 1
        yy_string = str(yy_int)[-2:]
        yy_next_str = str(yy_next_int)[-2:]
        return yy_string, yy_next_str

    def parse_data(self):
        headers = {
            "Accept": "application/json"
        }
        response = requests.get(url=self.api_url, headers=headers)
        data = response.text
        return json.loads(data)

    def produce_data_dict(self, master_data):
        y1, y2 = self.produce_yy_strings()
        ticker_symbols = [elem + y1 for elem in self.ticker_roots]
        ticker_prospectives = [elem + y2 for elem in self.ticker_roots]
        ticker_symbols.extend(ticker_prospectives)
        futures_of_interest, symbols_of_interest = extract_dictionaries(tickers=ticker_symbols, master_data=master_data)
        symbols_of_interest.sort()
        for elem in symbols_of_interest:
            for d in futures_of_interest:
                if d["symbol"] == elem:
                    self.data_dict[elem + "_dict"] = d
                    futures_of_interest.remove(d)

    def update(self):
        log.debug("Updating...")
        parsed = self.parse_data()
        self.produce_data_dict(master_data=parsed)
        self.wks1.update_acell("B2", str(datetime.datetime.utcnow()))
        self.wks1.update_acell("B4", '"{:s}"'.format(self.price_definition))
        present_date = datetime.datetime.now().date()
        perp_dict, _ = extract_dictionaries(tickers=[self.perp], master_data=parsed)
        perp_dict = perp_dict[0]
        perp_price = perp_dict[self.price_definition]
        k = 1
        for _, d in self.data_dict.items():
            ticker_symbol = d["symbol"]
            expiry_date = dateutil.parser.parse(d["expiry"]).date()
            delta_lapse = (expiry_date - present_date).days
            future_price = d[self.price_definition]
            perc_diff = (future_price - perp_price) / perp_price
            annual_perc = (1 + perc_diff) ** (365.0 / delta_lapse) - 1
            price_delta = future_price - perp_price

            letter = num_alph_mapper[k]
            cell_list = self.wks1.range(letter + "5:" + letter + "13")
            str_01, str_02, str_03, str_04 = str(ticker_symbol), str(expiry_date), str(present_date), str(delta_lapse)
            str_05, str_06 = "${:,.2f}".format(perp_price), "${:,.2f}".format(future_price)
            str_07, str_08 = "{:.2f}%".format(perc_diff * 100), "{:.2f}%".format(annual_perc * 100)
            str_09 = "${:,.2f}".format(price_delta)
            str_vals = [str_01, str_02, str_03, str_04, str_05, str_06, str_07, str_08, str_09]
            for i, val in enumerate(str_vals):
                cell_list[i].value = val
            self.wks1.update_cells(cell_list)

            k += 1

    def run(self):
        i = 0
        while True:
            try:
                self.update()
            except gspread.exceptions.APIError as e:
                log.exception(e)
                self.gc.login()
                time.sleep(10)
            except json.decoder.JSONDecodeError as e:
                log.exception(e)
                time.sleep(10)
            except Exception as e:
                log.exception(e)
                raise e
            i += 1
            time.sleep(self.sleep_interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Google Sheet Updater Utility.")
    parser.add_argument('credentials', help="Google service account credentials file in .json format.")
    parser.add_argument('-w', '--workbook', help="Google Sheets workbook to update.",
                        default='bitcoin_extractions')
    parser.add_argument('-l', '--logfile', help="Program logile.", default='updater.log')
    parser.add_argument('-v', '--verbose', help="Generate detailed log. (Loglevel set to DEBUG.)",
                        action='store_true')
    args = parser.parse_args()

    if args.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    logging.basicConfig(filename=args.logfile, filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=loglevel)

    updater = GsheetUpdater(args.credentials, price_definition="markPrice", sleep_interval=0.1,
                            api_url="https://www.bitmex.com/api/v1/instrument/active",
                            ticker_roots=["XBTH", "XBTM", "XBTU", "XBTZ"], perpetual_name="XBTUSD",
                            workbook_name=args.workbook, wks_num=0)
    updater.run()
# In the terminal:
#   cd to the directory of this script.
#   Execute the following:
#       nohup ./bitmex_v2.py &
