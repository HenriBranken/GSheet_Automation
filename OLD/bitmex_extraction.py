"""
In this script we are going to rely on Windows Task Scheduler...
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
# Help setup the client that is going to interact with GSheets API to get &
# update the relevant data.
import requests
import json
import dateutil.parser
import datetime
# import schedule
# import time


price_definition = "markPrice"
# Other Options are as specified below:
# prevClosePrice, prevPrice24h, vwap, highPrice, lowPrice, lastPrice,
# lastPriceProtected, bidPrice, midPrice, askPrice, impactBidPrice,
# impactMidPrice, impactAskPrice, fairPrice, markPrice, indicativeSettlePrice


# ------------------------------------------------------------------------------
# Define some custom functions
# ------------------------------------------------------------------------------
def extract_dictionaries(tickers, master_data):
    to_be_returned = []
    for entry in master_data:
        if entry["symbol"] in tickers:
            to_be_returned.append(entry)
    return to_be_returned


def infer_datetime(symbol_string):
    yymmdd = symbol_string[-6:]
    yyyy = "20" + yymmdd[0:2]
    mm = yymmdd[2:4]
    dd = yymmdd[4:6]
    expiry_dt = datetime.date(year=int(yyyy), month=int(mm), day=int(dd))
    return expiry_dt
# ------------------------------------------------------------------------------


scope = ['https://www.googleapis.com/auth/drive']  # give us full access
# Let us now define the credentials:
f_cred = "C:\\Users\\Administrator\\Documents\\Henri\\Nick_Levenstein\\CREDENTIALS.json"
credentials = ServiceAccountCredentials.\
    from_json_keyfile_name(f_cred, scope)

# We define a client:
gc = gspread.authorize(credentials)
# We are opening a worksheet:
sh = gc.open("bitcoin_extractions")

# Get the different sheets.  Sheet1 <> get_worksheet(0).
# Sheet2 <> get_worksheet(1).
wks1 = sh.get_worksheet(0)

present_utc_timestamp = datetime.datetime.utcnow()
present_date = datetime.datetime.now().date()
headers = {
    "Accept": "application/json"
}

api_endpoint = "https://www.bitmex.com/api/v1/instrument/active"
response = requests.get(url=api_endpoint, headers=headers)
data = response.text
parsed = json.loads(data)


indices = ["XBTUSD", "XBTH20", "XBTM20"]
dicts_of_interest = extract_dictionaries(tickers=indices,
                                         master_data=parsed)

data_dict = dict()
for i, index in enumerate(indices):
    data_dict[index + "_dict"] = dicts_of_interest[i]

# Extract the `Expiry` dates.
XBTH20_expiry_date = data_dict["XBTH20_dict"]["expiry"]
XBTH20_expiry_date = dateutil.parser.parse(XBTH20_expiry_date).date()

XBTM20_expiry_date = data_dict["XBTM20_dict"]["expiry"]
XBTM20_expiry_date = dateutil.parser.parse(XBTM20_expiry_date).date()

# Find the `Days Difference`
XBTH20_days_delta = (XBTH20_expiry_date - present_date).days
XBTM20_days_delta = (XBTM20_expiry_date - present_date).days

# Extract the prices for the Index, XBTZ19 future and XBTH20 future.
# FOR THE TIME BEING I AM USING "prevClosePrice" AS A PLACEHOLDER.
bitcoin_price = data_dict["XBTUSD_dict"][price_definition]
XBTH20_price = data_dict["XBTH20_dict"][price_definition]
XBTM20_price = data_dict["XBTM20_dict"][price_definition]

"""
# ------------------------------------------------------------------------------
# Populate the BitMEX Excel sheet.
# ------------------------------------------------------------------------------
wks1.update_acell("B2", str(present_utc_timestamp))
wks1.update_acell("B6", str(XBTH20_expiry_date))
wks1.update_acell("C6", str(XBTM20_expiry_date))
wks1.update_acell("B7", str(present_date))
wks1.update_acell("C7", str(present_date))
wks1.update_acell("B8", str(XBTH20_days_delta))
wks1.update_acell("C8", str(XBTM20_days_delta))
wks1.update_acell("B9", str(bitcoin_price))
wks1.update_acell("C9", str(bitcoin_price))
wks1.update_acell("B10", str(XBTH20_price))
wks1.update_acell("C10", str(XBTM20_price))
"""
