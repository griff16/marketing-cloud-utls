# ref source: https://medium.com/swlh/how-to-import-data-to-salesforce-marketing-cloud-exacttarget-using-python-rest-api-1302a26f89c0

import json
import requests
import sys
from time import time
from datetime import datetime
from math import floor


class Model:

    # consturctor
    def __init__(self):
        # ========= 変更箇所 ==========
        self.et_client_id = 'vzwdmunxldim9v323qn8qk8o'
        self.et_client_secret = '2nbzYMVs72nRvanqmUmKIjWl'
        self.et_subdomain = 'mc92gszf616ms75bpfrsqfr84m01'
        self.et_mid = '518001268'
        # ========= 変更箇所 ==========
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
                f'Unable to validate (ClientID/ClientSecret): {repr(response)}')

        access_token = response['access_token']
        expires_in = time() + response['expires_in']

        return access_token, expires_in  # return access_token and token expiration time


    # validate email (https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/validateEmail.htm)
    def email_validate(self, token, target_email):
        endpoint = '.rest.marketingcloudapis.com/address/v1/validateEmail'
        payload = {
            "email": target_email,
            "validators": ["SyntaxValidator", "MXValidator", "ListDetectiveValidator"]
        }
        headers = {'authorization': f'Bearer {token}'}

        response = requests.post(
            self.base_url + endpoint, data=payload, headers=headers).json()

        return f"The email [{response['email']}] is valid" if response['valid'] \
            else f"The validity of email [{response['email']}] is {str(response['valid'])} becuase of {response['failedValidation']}"


    # upsert data to DE (https://developer.salesforce.com/docs/atlas.en-us.noversion.mc-apis.meta/mc-apis/updateDataExtensionIDAsync.htm)
    def insert_data(self, access_token, expires_in, de_externalkey, data):
        endpoint = f'.rest.marketingcloudapis.com/data/v1/async/dataextensions/key:{de_externalkey}/rows'
        headers = {'authorization': f'Bearer {access_token}'}
        batch_size = self.get_batch_size(data[0])

        for batch in range(0, len(data), batch_size):
            if expires_in < time() + 60:
                expires_in, access_token = self.request_token()

            batch_data = data[batch : batch + batch_size]
            insert_request = requests.post(
                url=self.base_url + endpoint,
                data=json.dumps({'items': batch_data}, default=self.datetime_converter),
                headers=headers
            )

        if insert_request.status_code not in (200, 202):
            raise Exception(f'Insertion failed with message: {insert_request.json()}')
            insert_request.close()

        return 'complete'


    def create_data(self, base_id, base_email, size):
        pass


    # helper function
    def datetime_converter(self, value: datetime):
        if isinstance(value, datetime):
            return value.__str__()

    # helper function for 5MB limit for MC REST API
    def get_batch_size(self, record):
        batch = json.dumps({'items': record}, default=self.datetime_converter)
        return floor(4000 / (sys.getsizeof(batch) / 1024))


# main function
def main():
    m = Model()
    access_token, expires_in = m.request_token()
    # result = m.email_validate(access_token, target_email='x.zhang@salesforce.com')

    data = [
        {
            'id': 'api_test_python_001',
            'email': 'help@example.com',
        },
        {
            'id': 'api_test_python_002',
            'email': 'help2@example.com',
        }
    ]

    print(m.insert_data(access_token, expires_in, de_externalkey='FA8001D6-53D7-442A-A205-80F8E937441E', data=data))


# entry point
if __name__ == '__main__':
    main()
