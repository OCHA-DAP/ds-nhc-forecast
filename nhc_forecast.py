#!/usr/bin/python
"""
Generic Blob into HDX Pipeline:
------------

TODO
- Add summary about this dataset pipeline

"""
import logging
from hdx.utilities.downloader import DownloadError
from bs4 import BeautifulSoup
from dateutil import parser
from lat_lon_parser import parse
from azure.storage.blob import BlobServiceClient, ContentSettings
import pandas as pd
import os,io

logger = logging.getLogger(__name__)


class AzureBlobUpload:

    def upload_file(
            self,
            dataset_name: str,
            account: str,
            container: str,
            key: str,
            data: None
    ) -> str:
        """Upload file to blob storage
        Args:
            data (str): data to be uploaded
            account (str): Storage account
            container (str): Name of the container where the file will be uploaded to.
            key (str): Access key to container

        Returns:
            str: Path of the uploaded file
        """

        blob_service = BlobServiceClient.from_connection_string(
            f"DefaultEndpointsProtocol=https;AccountName={account};AccountKey= "
            f"{key};EndpointSuffix=core.windows.net")

        blob_client = blob_service.get_blob_client(container=container,
                                                   blob=f"{dataset_name}.csv")

        try:
            stream = io.StringIO()
            df = pd.DataFrame(data[dataset_name])
            df.to_csv(stream, sep=";", index=False)
            file_to_blob = stream.getvalue()
            blob_client.upload_blob(file_to_blob, overwrite=True,
                                    content_settings=ContentSettings(content_type="text/csv"))
        except Exception:
            logger.error(f"Failed to upload dataset: {dataset_name}")


def get_valid_time_in_datetime(validTime, issuance):
    issuance_datetime = parser.parse(issuance)

    if "/" in validTime:
        validTime = validTime.split("/")

    day = int(validTime[0])
    hour = int(validTime[1][:2])
    minute = int(validTime[1][2:4])
    newdatetime = issuance_datetime.replace(day=day,
                                            hour=hour,
                                            minute=minute)

    return newdatetime.strftime('%Y-%m-%dT%H:%M:%SZ')


class NHCHurricaneForecast:
    def __init__(self, configuration, retriever, folder, errors):
        self.configuration = configuration
        self.retriever = retriever
        self.folder = folder
        self.dataset_data = {}
        self.errors = errors
        self.created_date = None

    def get_data(self):
        """
         Expected formats for (full example available here https://www.nhc.noaa.gov/productexamples/NHC_JSON_Sample.json):
         "activeStorms": [
            {
                "id": "al062023",
                "binNumber": "AT1",
                "name": "Gert",
                "classification": "TD",
                "intensity": "25",
                "pressure": "1008",
                "latitude": "17.1N",
                "longitude": "58.4W",
                "latitudeNumeric": 17.1,
                "longitudeNumeric": -58.4,
                "movementDir": 285,
                "movementSpeed": 9,
                "lastUpdate": "2023-08-22T03:00:00.000Z",
                "publicAdvisory": {
                    "advNum": "011",
                    "issuance": "2023-08-22T03:00:00.000Z",
                    "fileUpdateTime": "2023-08-22T02:55:52.439Z",
                    "url": "https://www.nhc.noaa.gov/text/MIATCPAT1.shtml"
                },...}

        Expected formats for Forecast Advisory (full example here https://www.nhc.noaa.gov/productexamples/TCM_example.txt):
            FORECAST VALID 30/0000Z 22.9N  68.1W
            MAX WIND  85 KT...GUSTS 105 KT.
            64 KT... 15NE  10SE   0SW  10NW.
            50 KT... 30NE  20SE  10SW  20NW.
            34 KT... 80NE  50SE  30SW  60NW.
        :return:
        """
        base_url = self.configuration["base_url"]

        nhc_forecasts = self.retriever.download_json(base_url)

        # Check if there's data
        if not nhc_forecasts["activeStorms"]:
            logger.info(f"There are no active storms happening right now.")
            return None

        self.dataset_data["observed_tracks"] = []
        self.dataset_data["forecasted_tracks"] = []

        for observed_tracks in nhc_forecasts["activeStorms"]:
            self.dataset_data["observed_tracks"].append(
                {"id": observed_tracks["id"],
                 "name": observed_tracks["name"],
                 "basin": observed_tracks["id"][:2],
                 "intensity": int(observed_tracks["intensity"]),
                 "pressure": int(observed_tracks["pressure"]),
                 "latitude": observed_tracks["latitudeNumeric"],
                 "longitude": observed_tracks["longitudeNumeric"],
                 "lastUpdate": observed_tracks["lastUpdate"]})

        for storm in nhc_forecasts["activeStorms"]:
            id = storm["id"]
            name = storm["name"]
            issuance = storm["forecastAdvisory"]["issuance"]
            url = storm["forecastAdvisory"]["url"]
            try:
                forecast_html_file = self.retriever.download_text(url)
            except DownloadError:
                logger.error(f"Could not download from url {url}")

            forecast_file = BeautifulSoup(forecast_html_file, 'html.parser').find_all("pre")

            # Preprocessing the html file
            forecast_file = forecast_file[0].text.split("\n")

            latitude = longitude = maxwind = validTime = None
            for ln in forecast_file:

                # Preprocessing the line
                ln = ln.replace("...", " ")
                ln = ln.replace("  ", " ")
                forecast_line = ln.split(" ")

                if ln.startswith("FORECAST VALID") and len(forecast_line) >= 5:
                    # TODO parse the values below before adding to blob
                    validTime = get_valid_time_in_datetime(forecast_line[2], issuance)
                    latitude = forecast_line[3]
                    longitude = forecast_line[4]
                elif ln.startswith("REMNANTS OF CENTER LOCATED NEAR") and len(forecast_line) >= 8:
                    validTime = get_valid_time_in_datetime(forecast_line[8], issuance)
                    latitude = forecast_line[5]
                    longitude = forecast_line[6]

                if ln.startswith("MAX WIND") and len(forecast_line) >= 5:
                    maxwind = forecast_line[2]

                if latitude and longitude and maxwind:
                    forecast = {"id": id,
                                "name": name,
                                "issuance": issuance,
                                "basin": id[:2],
                                "latitude": parse(latitude),
                                "longitude": parse(longitude),
                                "maxwind": int(maxwind),
                                "validTime": validTime}
                    self.dataset_data["forecasted_tracks"].append(forecast)
                    maxwind = latitude = longitude = validTime = None

        return ["observed_tracks", "forecasted_tracks"]

    def upload_dataset(self, dataset_names):

        try:
            account = os.environ["STORAGE_ACCOUNT"]
            container = os.environ["CONTAINER"]
            key = os.environ["KEY"]
        except Exception:
            account = self.configuration["account"]
            container = self.configuration["container"]
            key = self.configuration["key"]

        aub = AzureBlobUpload()
        aub.upload_file(dataset_name=dataset_names[0],
                        account=account,
                        container=container,
                        key=key,
                        data=self.dataset_data)

        aub.upload_file(dataset_name=dataset_names[1],
                        account=account,
                        container=container,
                        key=key,
                        data=self.dataset_data)

        return dataset_names
