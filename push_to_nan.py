#!/usr/bin/env python3
import argparse
import configparser
import logging
import sys
import time
import os

import globus_sdk
import webbrowser

from globus_sdk import GlobusAPIError, TransferAPIError, TransferData

class PushToNan:

    def __init__(self,config):
        self.logger = logging.getLogger(PushToNan.__name__)
        self.config = config
        self.REFRESH = None
        client_id = config.get('login','client id')
        self.client =  globus_sdk.NativeAppAuthClient(client_id)
        self.client.oauth2_start_flow(refresh_tokens=True)
        gdata = config['globus']
        self.source_id = gdata['source endpoint']
        self.dest_id = gdata['dest endpoint']
        self.source_folder = gdata['source folder']
        self.dest_folder = gdata['dest folder']
        self.transfer_label = gdata['transfer label']
        poll_string = gdata['poll time seconds'].split('#')[0]
        self.poll_time = int(poll_string)


    def connect(self):
        lg = self.config['login']
        self.refresh_token_file = refresh_token_file = lg['refresh token file']
        have_refresh= os.path.exists(refresh_token_file)
        if have_refresh:
            with open(refresh_token_file) as f:
                token = f.readline().strip()
            self.authorizer = globus_sdk.RefreshTokenAuthorizer(refresh_token=token, auth_client=self.client)
            return #have a refresh token, all we need
        else:
            aurl = self.client.oauth2_get_authorize_url()
            browser = lg.getboolean('browser')
            if browser:
                webbrowser.open(aurl)
                time.sleep(0.5)
            else:
                print("open url {}".format(aurl))
                print("login and paste result")
        auth_code = input("Paste globus response:  ")
        tokens = self.client.oauth2_exchange_code_for_tokens(auth_code)
        transfer_data = tokens.by_resource_server['transfer.api.globus.org']
        TRANSFER_TOKEN = transfer_data['access_token']
        if not have_refresh:
            refresh = transfer_data['refresh_token']
            with open(refresh_token_file,'w') as f:
                print("{}".format(refresh),file=f)
        self.authorizer = globus_sdk.AccessTokenAuthorizer(TRANSFER_TOKEN)

    def _check_end_point(self, endpoint, path):
        """Check the endpoint path exists"""
        try:
           self.transfer_client.operation_ls(endpoint, path=path)
        except TransferAPIError as tapie:
            raise ValueError('Failed to query endpoint "{}": {}'.format(
                endpoint,
                tapie.message
            ))

    def transfer(self):
        self.transfer_client = transfer_client = globus_sdk.TransferClient(authorizer=self.authorizer)
        try:
            transfer_client.endpoint_autoactivate(self.source_id)
            transfer_client.endpoint_autoactivate(self.dest_id)
        except GlobusAPIError as ex:
            if ex.http_status == 401:
                sys.exit('Refresh token has expired. '
                         'Please delete the `tokens` object from '
                         '{} and try again.'.format(self.refresh_token_file))
            else:
                raise ex

        while True:
            self._check_end_point(self.source_id, self.source_folder)
            self._check_end_point(self.dest_id, self.dest_folder)
            tdata = TransferData(self.transfer_client, self.source_id, self.dest_id, label=self.transfer_label,
                                 sync_level="checksum")
            tdata.add_item(self.source_folder, self.dest_folder, recursive=True)
            task = transfer_client.submit_transfer(tdata)
            task_id = task['task_id']
            self.logger.info("Task id {} submitted".format(task_id))
            transfer_client.task_wait(task_id=task_id,timeout=self.poll_time,polling_interval=5)
            td = transfer_client.get_task(task_id)
            self.logger.info("Task id {} complete".format(task_id))
            self.logger.debug(td)
            time.sleep(self.poll_time)




if __name__ == "__main__":
    DEFAULT_CFG = "nanglobus.cfg"
    parser = argparse.ArgumentParser()
    parser.add_argument('-c',"--config",default=DEFAULT_CFG,help="Config file to use. Default: {}".format(DEFAULT_CFG))
    parser.add_argument('-l',"--loglevel",default='INFO',help="Python logging level. Default INFO")
    args = parser.parse_args()
    logging.basicConfig()
    config = configparser.ConfigParser( )
    with open(args.config) as f:
        config.read_file(f)
    pton = PushToNan(config)
    pton.logger.setLevel(getattr(logging,args.loglevel))
    pton.connect()
    try:
        pton.transfer()
    except:
        pton.logger.exception("Transfer failed")


