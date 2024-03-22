import pandas as pd
import json
import requests
import numpy as np
url = "https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0?offset=0&count=true&results=true&schema=true&keys=true&format=json&rowIds=false"

combine_response_data = []
retry_count = 0
max_retries = 3

while  retry_count < max_retries:
  try:

    response = requests.get(url)
    if response.status_code == 200:
      total_records = 2000
      counter = 0

      while counter < total_records:
        limit = 500
        offset_url = f"https://data.cms.gov/provider-data/api/1/datastore/query/mj5m-pzi6/0?offset={counter}&count=true&results=true&schema=true&keys=true&format=json&rowIds=false"
        offset_respnse = requests.get(offset_url)
        if offset_respnse.status_code == 200:
          print(f"made request {limit} results at offset {counter}")
          # offset_data = list(map(lambda data: json.dumps(data), offset_respnse.json()["results"]))
          offset_data = offset_respnse.json()["results"]

          combine_response_data.extend(offset_data)
        else:
          print("error getting data from api")
        counter += limit # counter = counter + limit --> ( 0 = 0 + 500 => 500)

      # if len(combine_response_data) <=2000:
      #     break

    else:
      print(f"request failed with status code {response.status_code}")
  except requests.exceptions.RequestException as Error:
    print(f"Attempt {retry_count +1 }/{max_retries} failed: {Error}")
    pass
  else:
    break

if retry_count == max_retries:
  print(f"Request failed 3 times")
  if response is not None:
    print(f"Response content: {response.content}")

else:
  data_frame = pd.DataFrame(combine_response_data)
  data_frame.to_csv("provider_data.csv")
  # print(data_frame)


