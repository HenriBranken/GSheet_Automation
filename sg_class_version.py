#!/usr/bin/python3

import gspread
from oauth2client.service_account import ServiceAccountCredentials
import time
import logging


class gsheet_updater():

    def __init__(self):
        self.logfile = open('updater.log', 'w')
        self.logger = logging.getLogger(__name__)

        self.logger.info("Initializing gsheet_updater...")
        self.authenticate()

    def authenticate(self):
        scope = ['https://www.googleapis.com/auth/drive']
        credentials_file = 'sg_service_credentials.json'
        credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        self.gc = gspread.authorize(credentials)

        self.sh = self.gc.open('demo_sheet')
        self.wsh = self.sh.get_worksheet(0)

    def update(self, i):
        self.logger.info("Updating %d" % (i))
        self.wsh.update_acell('A1', i)

    def run(self):
        i = 0
        while True:
            try:
                self.update(i)
            except gspread.exceptions.APIError as e:
                self.logger.exception(e)
                self.gc.login()
            except Exception as e:
                self.logger.exception(e)
                raise e
            i += 1
            time.sleep(1)


if __name__ == '__main__':
    logging.basicConfig(filename='updater.log', filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=logging.INFO)

    updater = gsheet_updater()
    updater.run()
