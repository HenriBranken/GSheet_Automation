"""
In this script we are going to rely on Windows Task Scheduler...
Have a look at `get_stocks.bat`
"""

import gspread
from oauth2client.service_account import ServiceAccountCredentials
# Help setup the client that is going to interact with GSheets API to get &
# update the relevant data.
import requests
import json
import dateutil.parser
import datetime
import schedule
import time


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
wks0 = sh.get_worksheet(0)

present_utc_timestamp = datetime.datetime.utcnow()
present_date = datetime.datetime.now().date()
headers = {
    "Accept": "application/json"
}

kraken_api_endpoint = "https://futures.kraken.com/derivatives/api/v3/tickers"

r = requests.get(url=kraken_api_endpoint, headers=headers)
data = r.text
parsed = json.loads(data)
parsed = parsed["tickers"]
# print(json.dumps(parsed, indent=2))

indices = ["pi_xbtusd", "fi_xbtusd_191227"]
kraken_dicts_of_interest = extract_dictionaries(tickers=indices,
                                                master_data=parsed)

kraken_dict = dict()
for i, index in enumerate(indices):
    kraken_dict[index + "_dict"] = kraken_dicts_of_interest[i]

fi_xbtusd_191227_bid = kraken_dict["fi_xbtusd_191227_dict"]["bid"]
fi_xbtusd_191227_ask = kraken_dict["fi_xbtusd_191227_dict"]["ask"]
fi_xbtusd_191227_expiry_date = infer_datetime(symbol_string=indices[1])
fi_xbtusd_191227_days_delta = (fi_xbtusd_191227_expiry_date - present_date).days

# Notice that the following index never expires.
pi_xbtusd_bid = kraken_dict["pi_xbtusd_dict"]["bid"]
pi_xbtusd_ask = kraken_dict["pi_xbtusd_dict"]["ask"]

# ------------------------------------------------------------------------------
# Populate the Kraken Google Sheet
# ------------------------------------------------------------------------------
wks0.update_acell("B2", str(present_utc_timestamp))
wks0.update_acell("B3", str(kraken_api_endpoint))
wks0.update_acell("B6", str(present_date))
wks0.update_acell("B8", str(pi_xbtusd_bid))
wks0.update_acell("B9", str(pi_xbtusd_ask))

wks0.update_acell("D5", str(fi_xbtusd_191227_expiry_date))
wks0.update_acell("D6", str(present_date))
wks0.update_acell("D8", str(fi_xbtusd_191227_bid))
wks0.update_acell("D9", str(fi_xbtusd_191227_ask))
wks0.update_acell("D12", str(fi_xbtusd_191227_days_delta))
