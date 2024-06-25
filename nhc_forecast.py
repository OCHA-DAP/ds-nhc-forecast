#!/usr/bin/python
"""
------------

NHC Forecast
- This pipeline extracts data from NHC and appends whenever there is an update
 to the observed tracks or forecasted tracks.

"""
import logging
import requests
from hdx.utilities.downloader import DownloadError
from bs4 import BeautifulSoup
from dateutil import parser
from lat_lon_parser import parse
from azure.storage.blob import BlobServiceClient, ContentSettings
import pandas as pd
import os,io
import time

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

        blob_client = blob_service.get_blob_client(
            container=container,
            blob=f"noaa/nhc/{dataset_name}.csv")

        try:
            stream = io.StringIO()
            df = pd.DataFrame(data)
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


def trigger_for_active_storms(url, key):

    headers = {"Accept": "application/vnd.github.v3+json",
               "Authorization": key,
               "Content-Type": "application/json"
               }
    data = '{"ref":"' + "main" + '"}'

    resp = requests.post(url, headers=headers, data=data)

    return resp


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
        trigger_hti_hurricanes_action = False

        nhc_forecasts = self.retriever.download_json(base_url)

        # Check if there's data
        if not nhc_forecasts["activeStorms"]:
            logger.info(f"There are no active storms happening right now.")
            return None
        else:
            trigger_hti_hurricanes_action = True

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

        return ["observed_tracks", "forecasted_tracks"], trigger_hti_hurricanes_action

    def upload_dataset(self, dataset_names, trigger_hti_hurricanes_action):

        try:
            account = os.environ["STORAGE_ACCOUNT"]
            container = os.environ["CONTAINER"]
            key = os.environ["KEY"]
            ghp = os.environ["GHP"]
            ghaction_url = os.environ["GH_ACTION_URL"]
        except Exception:
            account = self.configuration["account"]
            container = self.configuration["container"]
            key = self.configuration["key"]
            ghp = self.configuration["ghp"]
            ghaction_url = self.configuration["ghaction_url"]

        forecasted_tracks_blob = self.retriever.download_file(
            url="test",
            account=account,
            container=container,
            key=key,
            blob="noaa/nhc/forecasted_tracks.csv")

        observed_tracks_blob = self.retriever.download_file(
            url="test",
            account=account,
            container=container,
            key=key,
            blob="noaa/nhc/observed_tracks.csv")

        forecasted_tracks = self.dataset_data["forecasted_tracks"]
        stream = io.StringIO()
        forecasted_tracks = pd.DataFrame(forecasted_tracks)
        forecasted_tracks.to_csv(stream, sep=";", index=False)
        forecasted_tracks_df = pd.read_csv(forecasted_tracks_blob, sep=";", escapechar='\\')

        observed_tracks = self.dataset_data["observed_tracks"]
        stream = io.StringIO()
        observed_tracks = pd.DataFrame(observed_tracks)
        observed_tracks.to_csv(stream, sep=";", index=False)
        observed_tracks_df = pd.read_csv(observed_tracks_blob, sep=";", escapechar='\\')

        forecasted_tracks_append = pd.concat([forecasted_tracks_df, forecasted_tracks]).drop_duplicates().reset_index(drop=True)
        observed_tracks_append = pd.concat([observed_tracks_df, observed_tracks]).drop_duplicates().reset_index(drop=True)

        aub = AzureBlobUpload()

        # Adding previous file to backup folder
        dstr = time.strftime("%Y%m%d")
        tstr = time.strftime("%H%M%S")
        aub.upload_file(dataset_name=f"previous/{dstr}_{tstr}/forecasted_tracks",
                        account=account,
                        container=container,
                        key=key,
                        data=forecasted_tracks_df)
        aub.upload_file(dataset_name=f"previous/{dstr}_{tstr}/observed_tracks",
                        account=account,
                        container=container,
                        key=key,
                        data=observed_tracks_df)

        # Adding new rows to be appended in historical file
        aub.upload_file(dataset_name="forecasted_tracks",
                        account=account,
                        container=container,
                        key=key,
                        data=forecasted_tracks_append)

        aub.upload_file(dataset_name="observed_tracks",
                        account=account,
                        container=container,
                        key=key,
                        data=observed_tracks_append)

        if trigger_hti_hurricanes_action:
            logger.info("Triggering Haiti Hurricanes action")
            trigger_for_active_storms(ghaction_url, ghp, observed_tracks_df["id"], observed_tracks["id"])

        return dataset_names
