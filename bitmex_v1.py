"""
In this script we are going to rely on Windows Task Scheduler...
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
# Help setup the client that is going to interact with GSheets API to get &
# update the relevant data.
import requests
import json
import datetime
import dateutil.parser
import os
# import schedule
# import time


price_definition = "markPrice"
# All the options are as specified below:
# prevClosePrice, prevPrice24h, vwap, highPrice, lowPrice, lastPrice,
# lastPriceProtected, bidPrice, midPrice, askPrice, impactBidPrice,
# impactMidPrice, impactAskPrice, fairPrice, markPrice, indicativeSettlePrice


# ----------------------------------------------------------------------------------------------------------------------
# Define some custom functions
# ----------------------------------------------------------------------------------------------------------------------
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
# ----------------------------------------------------------------------------------------------------------------------


# Give us full access to editing the Google Sheets Document
scope = ['https://www.googleapis.com/auth/drive']
# Let us now define the credentials:
if os.name == "nt":
    f_cred = "C:\\Users\\Administrator\\Documents\\Henri\\Nick_Levenstein\\CREDENTIALS.json"
else:
    f_cred = "/home/henri/stuff/matogen/Nicholas_Levenstein/GSheet_Automation/CREDENTIALS.json"
credentials = ServiceAccountCredentials.from_json_keyfile_name(f_cred, scope)

# We define a client as follows:
gc = gspread.authorize(credentials)
# We open a worksheet as follows:
sh = gc.open("bitcoin_extractions")

# Get the correct Sheet from the spreadsheet book.
# Sheet1 <> get_worksheet(0).
# Sheet2 <> get_worksheet(1).
wks1 = sh.get_worksheet(0)

# Extract the current timestamp and current date.
present_utc_timestamp = datetime.datetime.utcnow()
present_date = datetime.datetime.now().date()

# Get the last two digits from the "YYYY" format.
yy_int = present_date.year
yy_string = str(yy_int)[-2:]
yy_next_int = yy_int + 1
yy_next_str = str(yy_next_int)[-2:]

# Parse the data extracted from the API.
api_endpoint = "https://www.bitmex.com/api/v1/instrument/active"
headers = {
    "Accept": "application/json"
}
response = requests.get(url=api_endpoint, headers=headers)
data = response.text
parsed = json.loads(data)

# Define the ticker symbols for all the possible futures.
ticker_roots = ["XBTH", "XBTM", "XBTU", "XBTZ"]
# H = March.  M = June.  U = September.  Z = December.
ticker_symbols = [elem + yy_string for elem in ticker_roots]
ticker_prospectives = [elem + yy_next_str for elem in ticker_roots]
ticker_symbols.extend(ticker_prospectives)

# Extract all the data needed.
# futures_of_interest is a list of dictionaries.
# symbols_of_interest is a list of symbols.
futures_of_interest, symbols_of_interest = extract_dictionaries(tickers=ticker_symbols, master_data=parsed)
symbols_of_interest.sort()
xbtusd_dict, _ = extract_dictionaries(tickers=["XBTUSD"], master_data=parsed)
xbtusd_dict = xbtusd_dict[0]
xbtusd_price = xbtusd_dict[price_definition]

# Create the dictionary structure: {"ticker1_dict": {...}, "ticker2_dict": {...}, etc...}
data_dict = dict()
for elem in symbols_of_interest:
    for d in futures_of_interest:
        if d["symbol"] == elem:
            data_dict[elem + "_dict"] = d
            futures_of_interest.remove(d)

num_alph_mapper = {
    1: "B",
    2: "C",
    3: "D",
    4: "E",
    5: "F"
}

# Make some updates to the spreadsheet that do not change regardless of the ticker considered.
wks1.update_acell("B2", str(present_utc_timestamp))
wks1.update_acell("B4", '"{:s}"'.format(price_definition))

# Iteratively update the columns.  One column per ticker.
k = 1
for _, d in data_dict.items():
    ticker_symbol = d["symbol"]
    expiry_date = dateutil.parser.parse(d["expiry"]).date()
    delta_lapse = (expiry_date - present_date).days
    future_price = d[price_definition]
    perc_diff = (future_price - xbtusd_price)/xbtusd_price
    annual_perc = (1 + perc_diff)**(365.0/delta_lapse) - 1
    price_delta = future_price - xbtusd_price

    letter = num_alph_mapper[k]
    wks1.update_acell(letter+"5", str(ticker_symbol))
    wks1.update_acell(letter+"6", str(expiry_date))
    wks1.update_acell(letter+"7", str(present_date))
    wks1.update_acell(letter+"8", str(delta_lapse))
    wks1.update_acell(letter+"9", "${:,.2f}".format(xbtusd_price))
    wks1.update_acell(letter+"10", "${:,.2f}".format(future_price))
    wks1.update_acell(letter+"11", "{:.2f}%".format(perc_diff*100))
    wks1.update_acell(letter+"12", "{:.2f}%".format(annual_perc*100))
    wks1.update_acell(letter+"13", "${:,.2f}".format(price_delta))
    k += 1
