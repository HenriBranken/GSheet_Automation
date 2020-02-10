import gspread
from oauth2client.service_account import ServiceAccountCredentials
# Help setup client that is going to interact with GSheets API to get & update
# the relevant data.
import requests
import json
import dateutil.parser
import datetime

scope = ['https://www.googleapis.com/auth/drive']

# Let us now define the credentials:
credentials = ServiceAccountCredentials.\
    from_json_keyfile_name("CREDENTIALS.json", scope)

gc = gspread.authorize(credentials)  # We define a client.

sh = gc.open("my_experiment")  # We are opening a worksheet.

wks = sh.get_worksheet(1)

wks0 = sh.get_worksheet(0)


# -----------------------------------------------------------------------------
# Define some custom functions
# -----------------------------------------------------------------------------
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
# -----------------------------------------------------------------------------


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# The First Part.  BitMEX
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
headers = {
    "Accept": "application/json"
}

present_utc_timestamp = datetime.datetime.utcnow()

api_endpoint = "https://www.bitmex.com/api/v1/instrument/active"
response = requests.get(url=api_endpoint, headers=headers)
data = response.text
parsed = json.loads(data)


indices = ["XBTUSD", "XBTZ19", "XBTH20"]
dicts_of_interest = extract_dictionaries(tickers=indices,
                                         master_data=parsed)

data_dict = dict()
for i, index in enumerate(indices):
    data_dict[index + "_dict"] = dicts_of_interest[i]
# print(json.dumps(data_dict, indent=2))

# Extract the `Expiry` dates.
XBTZ19_expiry_date = data_dict["XBTZ19_dict"]["expiry"]
XBTZ19_expiry_date = dateutil.parser.parse(XBTZ19_expiry_date).date()

XBTH20_expiry_date = data_dict["XBTH20_dict"]["expiry"]
XBTH20_expiry_date = dateutil.parser.parse(XBTH20_expiry_date).date()

# Extract today's date: Today/Present Date.
present_date = datetime.datetime.now().date()

# Find the `Days Difference`
XBTZ19_days_delta = (XBTZ19_expiry_date - present_date).days
XBTH20_days_delta = (XBTH20_expiry_date - present_date).days

# Extract the prices for the Index, XBTZ19 future and XBTH20 future.
# FOR THE TIME BEING I AM USING "prevClosePrice" AS A PLACEHOLDER.
price_definition = "prevClosePrice"
bitcoin_price = data_dict["XBTUSD_dict"][price_definition]
XBTZ19_price = data_dict["XBTZ19_dict"][price_definition]
XBTH20_price = data_dict["XBTH20_dict"][price_definition]

# -----------------------------------------------------------------------------
# Populate the Excel sheet.
# -----------------------------------------------------------------------------
wks.update_acell("B2", str(present_utc_timestamp))
wks.update_acell("B6", str(XBTZ19_expiry_date))
wks.update_acell("C6", str(XBTH20_expiry_date))
wks.update_acell("B7", str(present_date))
wks.update_acell("C7", str(present_date))
wks.update_acell("B8", str(XBTZ19_days_delta))
wks.update_acell("C8", str(XBTH20_days_delta))
wks.update_acell("B9", str(bitcoin_price))
wks.update_acell("C9", str(bitcoin_price))
wks.update_acell("B10", str(XBTZ19_price))
wks.update_acell("C10", str(XBTH20_price))


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# The Second Part.  Kraken.
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
kraken_api_endpoint = "https://futures.kraken.com/derivatives/api/v3/tickers"

r = requests.get(url=kraken_api_endpoint, headers=headers)
data = r.text
parsed = json.loads(data)
parsed = parsed["tickers"]

# print(json.dumps(parsed, indent=2))

indices = ["pi_xbtusd", "fi_xbtusd_191129", "fi_xbtusd_191227"]
kraken_dicts_of_interest = extract_dictionaries(tickers=indices,
                                                master_data=parsed)

kraken_dict = dict()
for i, index in enumerate(indices):
    kraken_dict[index + "_dict"] = kraken_dicts_of_interest[i]

# Extract the Kraken Futures Expiry dates
fi_xbtusd_191129_expiry_date = infer_datetime(symbol_string=indices[1])
fi_xbtusd_191227_expiry_date = infer_datetime(symbol_string=indices[2])

# Find the `Days Difference` between futures expiry dates and present date
fi_xbtusd_191129_days_delta = (fi_xbtusd_191129_expiry_date - present_date).days
fi_xbtusd_191227_days_delta = (fi_xbtusd_191227_expiry_date - present_date).days

# Extract the prices
fi_xbtusd_191129_markPrice = kraken_dict["fi_xbtusd_191129_dict"]["markPrice"]
fi_xbtusd_191129_bid = kraken_dict["fi_xbtusd_191129_dict"]["bid"]
fi_xbtusd_191129_ask = kraken_dict["fi_xbtusd_191129_dict"]["ask"]
fi_xbtusd_191129_open24h = kraken_dict["fi_xbtusd_191129_dict"]["open24h"]
fi_xbtusd_191129_last = kraken_dict["fi_xbtusd_191129_dict"]["last"]

fi_xbtusd_191227_markPrice = kraken_dict["fi_xbtusd_191227_dict"]["markPrice"]
fi_xbtusd_191227_bid = kraken_dict["fi_xbtusd_191227_dict"]["bid"]
fi_xbtusd_191227_ask = kraken_dict["fi_xbtusd_191227_dict"]["ask"]
fi_xbtusd_191227_open24h = kraken_dict["fi_xbtusd_191227_dict"]["open24h"]
fi_xbtusd_191227_last = kraken_dict["fi_xbtusd_191227_dict"]["last"]

pi_xbtusd_markPrice = kraken_dict["pi_xbtusd_dict"]["markPrice"]
pi_xbtusd_bid = kraken_dict["pi_xbtusd_dict"]["bid"]
pi_xbtusd_ask = kraken_dict["pi_xbtusd_dict"]["ask"]
pi_xbtusd_open24h = kraken_dict["pi_xbtusd_dict"]["open24h"]
pi_xbtusd_last = kraken_dict["pi_xbtusd_dict"]["last"]

# -----------------------------------------------------------------------------
# Populate the Google Sheet
# -----------------------------------------------------------------------------

wks0.update_acell("B2", str(present_utc_timestamp))
wks0.update_acell("B3", kraken_api_endpoint)
wks0.update_acell("B6", str(present_date))
wks0.update_acell("B7", pi_xbtusd_markPrice)
wks0.update_acell("B8", pi_xbtusd_bid)
wks0.update_acell("B9", pi_xbtusd_ask)
wks0.update_acell("B10", pi_xbtusd_open24h)
wks0.update_acell("B11", pi_xbtusd_last)

wks0.update_acell("C5", str(fi_xbtusd_191129_expiry_date))
wks0.update_acell("C6", str(present_date))
wks0.update_acell("C7", fi_xbtusd_191129_markPrice)
wks0.update_acell("C8", fi_xbtusd_191129_bid)
wks0.update_acell("C9", fi_xbtusd_191129_ask)
wks0.update_acell("C10", fi_xbtusd_191129_open24h)
wks0.update_acell("C11", fi_xbtusd_191129_last)
wks0.update_acell("C12", fi_xbtusd_191129_days_delta)

wks0.update_acell("D5", str(fi_xbtusd_191227_expiry_date))
wks0.update_acell("D6", str(present_date))
wks0.update_acell("D7", fi_xbtusd_191227_markPrice)
wks0.update_acell("D8", fi_xbtusd_191227_bid)
wks0.update_acell("D9", fi_xbtusd_191227_ask)
wks0.update_acell("D10", fi_xbtusd_191227_open24h)
wks0.update_acell("D11", fi_xbtusd_191227_last)
wks0.update_acell("D12", fi_xbtusd_191227_days_delta)
