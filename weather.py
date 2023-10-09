import requests
import pandas as pd
import logging
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import service_account
from os import getenv
from dotenv import load_dotenv
import time

#Initializing logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

#Load API Key
load_dotenv()
API_KEY = getenv('KEY')

class GetRealTimeWeather:

    def __init__(self):
        self.url = "https://weatherapi-com.p.rapidapi.com/current.json"
        self.headers = {"X-RapidAPI-Key": API_KEY,
                        "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"}
        self.weather = pd.DataFrame(columns=["city", "region", "country", "location", "localtime", "last_updated", "temp_c", "temp_f", "wind_kph", "wind_mph", "precip_mm", "precip_in", "condition"])
        
    def get_cities(self) -> list:
        df = pd.read_csv("us_states_and_capitals.csv", sep=";", encoding='latin-1')
        return df["state_and_capital"]
        
    def get_row_values(self, data:dict) -> dict:
        return {
                "city": data["location"]["name"], 
                "region": data["location"]["region"],
                "country": data["location"]["country"],
                "location":str(data["location"]["lat"]) + ',' + str(data["location"]["lon"]),
                "localtime":data["location"]["localtime"],
                "last_updated":data["current"]["last_updated"],
                "temp_c":data["current"]["temp_c"], 
                "temp_f":data["current"]["temp_f"],
                "wind_kph":data["current"]["wind_kph"],
                "wind_mph":data["current"]["wind_mph"],
                "precip_mm":data["current"]["precip_mm"],
                "precip_in":data["current"]["precip_in"],
                "condition":data["current"]["condition"]["text"]
            }
    
    def append_row_to_dataframe(self, df:pd.DataFrame, new_row:dict) -> None:
        df.loc[len(df)] = new_row

    def export_df_as_csv(self, df:pd.DataFrame) -> None:
        filename = datetime.today().strftime('%Y%m%d%H%M%S')
        df.to_csv(f"weather_info_{filename}.csv", index=False)
        logger.info(f"{filename} was written as a .csv file")
         
    def extract_weather_data(self) -> pd.DataFrame:
        start = time.time()

        for city in self.get_cities():
            querystring = {"q":city}
            data = requests.get(url=self.url, headers=self.headers, params=querystring).json()
            row = self.get_row_values(data=data)
            self.append_row_to_dataframe(df=self.weather, new_row=row)
            logger.info(f"Weather for {city} was collected")
            time.sleep(0.5)
    
        self.export_df_as_csv(df=self.weather)
        end = time.time()
        logger.info(f"Total elapsed time: {round(end - start, ndigits=2)}s")
        
        return self.weather

class LoadToBQ:
    
    def __init__(self, data:pd.DataFrame):
        self.data = data

    def _get_bq_credentials(self):
        return service_account.Credentials.from_service_account_file('data-with-luan-credentials.json')

    def load(self) -> None:
        self.data.to_gbq(
            destination_table="us_weather_pipeline.current_us_weather", 
            project_id="data-with-luan", 
            if_exists="replace", 
            credentials=self._get_bq_credentials()
        )
        logger.info(f"current_us_weather_info loaded to BigQuery!")

if __name__ == '__main__':
    updated_weather = GetRealTimeWeather()
    data = updated_weather.extract_weather_data()
    LoadToBQ(data).load()