# -*- coding: utf-8 -*-
# ref source: https://medium.com/swlh/how-to-import-data-to-salesforce-marketing-cloud-exacttarget-using-python-rest-api-1302a26f89c0

import json
import requests
import sys
from time import time
from datetime import datetime
from math import floor


class Model:

    # consturctor
    def __init__(self, client_id, client_secret, subdomain, mid):
        self.et_client_id = client_id
        self.et_client_secret = client_secret
        self.et_subdomain = subdomain
        self.et_mid = mid
        self.base_url = 'https://' + self.et_subdomain


    # accquire access token and exipiration time of the token
    def request_token(self):
        endpoint = '.auth.marketingcloudapis.com/v2/token'
        payload = {
            'client_id': self.et_client_id,
            'client_secret': self.et_client_secret,
            'account_id': self.et_mid,
            'grant_type': 'client_credentials'
        }
        response = requests.post(self.base_url + endpoint, data=payload).json()

        if 'access_token' not in response:  # throw error if request is unsuccessful
            raise Exception(
                f"Unable to validate (ClientID/ClientSecret): {repr(response)}")

        access_token = response['access_token']
        expires_in = time() + response['expires_in']

        return access_token, expires_in  # return access_token and token expiration time


    # validate email (https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/validateEmail.htm)
    def email_validate(self, target_email):
        access_token, expires_in = self.request_token()
        endpoint = '.rest.marketingcloudapis.com/address/v1/validateEmail'
        payload = {
            "email": target_email,
            "validators": ["SyntaxValidator", "MXValidator", "ListDetectiveValidator"]
        }
        headers = {'authorization': f'Bearer {access_token}'}

        response = requests.post(self.base_url + endpoint, data=payload, headers=headers).json()

        return f"The email [{response['email']}] is valid" if response['valid'] \
            else f"The validity of email [{response['email']}] is {str(response['valid'])} becuase of {response['failedValidation']}"


    # upsert data to DE (https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/updateDataExtensionIDAsync.htm)
    # requires the DE to have primary key
    def upsert_data(self, de_externalkey, data):
        access_token, expires_in = self.request_token()
        endpoint = f'.rest.marketingcloudapis.com/data/v1/async/dataextensions/key:{de_externalkey}/rows'
        headers = {'authorization': f'Bearer {access_token}'}
        batch_size = self.get_batch_size(data[0])

        for batch in range(0, len(data), batch_size):
            if expires_in < time() + 60:
                expires_in, access_token = self.request_token()

            batch_data = data[batch: batch + batch_size]
            insert_request = requests.put(
                url=self.base_url + endpoint,
                data=json.dumps({'items': batch_data},
                                default=self.datetime_converter),
                headers=headers
            )

        if insert_request.status_code not in (200, 202):
            raise Exception(
                f'Insertion failed with message: {insert_request.json()}')

        return True


    # create [de_size] of {id: baseid_i, email: help+i@example.com} data points
    def create_data(self, base_id, base_email, de_size):
        data = []
        username, domain = base_email.split('@')

        for i in range(0, de_size):
            temp_data = {
                'id': f'{base_id}_{i + 1}',
                'email': f'{username}+{i + 1}@{domain}'
            }

            data.append(temp_data)

        return data

    # helper function
    def datetime_converter(self, value: datetime):
        if isinstance(value, datetime):
            return value.__str__()

    # helper function for 5MB limit for MC REST API
    def get_batch_size(self, record):
        batch = json.dumps({'items': record}, default=self.datetime_converter)
        return floor(4000 / (sys.getsizeof(batch) / 1024))


# parse parameters
def parse_param():
    try:
        client_id, client_secret, subdomain, mid  = input('Enter client_id, client_secret, subdomain, mid in order seperated by commas\n').split(',')
        client_id, client_secret, subdomain, mid = client_id.strip(), client_secret.strip(), subdomain.strip(), mid.strip()
    except Exception:
        print(f'\nsomething went wrong')
        return False, None

    return True, [client_id, client_secret, subdomain, mid]
    

# main function
def main():
    
    list = None
    while (True):
        result, list = parse_param()
        if (result):
            break
        print('Enter again\n')

    print(f'\nclient_id: {list[0]}')
    print(f'client_secret: {list[1]}')
    print(f'subdomain: {list[2]}')
    print(f'mid: {list[3]}')
    print('loading...') 

    m = Model(client_id=list[0], client_secret=list[1], subdomain=list[2], mid=list[3])
    while(True):
        try:
            print('\nPlease enter one of the following commands')
            inp = input(
                "\t1: Email Validation (e.g. 1, help@example.com)\n" + 
                "\t2: Upsert Data Points (e.g. 2, external_key, base_id, help@example.com, 10)\n" + 
                "\tq: Exit\n").split(',')

            cmd = inp[0].strip()
            if (cmd == '1'):
                result = m.email_validate(target_email=inp[1].strip())
                print('Result: ' + result)
                
            elif (cmd == '2'):
                data = m.create_data(base_id=inp[2].strip(), base_email=inp[3].strip(), de_size=int(inp[4].strip()))
                m.upsert_data(de_externalkey=inp[1].strip(), data=data)
                print('Upsert Completed')
            
            elif (cmd == 'q'):
                print('The program has ended')
                break
            
            else:
                print('unrecognized command. retype the command\n')
        
        except Exception as e:
            print(e)    
            print(f'\nsomething went wrong')


# entry point
if __name__ == '__main__':
    main()
