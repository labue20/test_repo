import requests
import json
import gc
import time
import pandas as pd

class GetDataFromAPI:

    def extract_data_from_cms(self):

        url = "https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0?offset=0&count=true&results=true&schema=true&keys=true&format=json&rowIds=false"

        all_data = []
        retry_count = 0
        max_retries = 3


        while retry_count < max_retries:
            try:
                response = requests.get(url)
                if response.status_code == 200:
                    # total_records = int(response.json()['count'])
                    total_records = 5000
                    counter = 0
                    while counter < total_records:
                        limit = 500
                        offset_url = f"https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0?offset={counter}&count=true&results=true&schema=true&keys=true&format=json&rowIds=false"
                        offset_response = requests.get(offset_url)
                        if offset_response.status_code == 200:
                            print(f"Made request {limit} results at offset {counter}")

                            offset_data = offset_response.json()["results"]
                            all_data.extend(offset_data)

                        else:
                            print("error getting data")
                        counter += limit
                        gc.collect
                else:
                    print(f"Request failed with status code:{response.status_code}")

            except requests.exceptions.RequestException as e:
                print(f"Attemp {retry_count +1}/{max_retries} failed: {e}")
            else:
                break
            retry_count += 1

        if retry_count == max_retries:
            print(f"Request failed 3 times. Failing task")
            if response is not None:
                print(f"Response content: {response.content}")
        else:
            df = pd.DataFrame(all_data)
            df.to_csv("provider_data.csv")
            print(df)



