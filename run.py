#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
import tempfile
from os.path import expanduser, join, exists
from typing import Any

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.retriever import Retrieve
from nhc_forecast import NHCHurricaneForecast
from datetime import datetime
import hmac
import hashlib
import base64

logger = logging.getLogger(__name__)
lookup = "ds-nhc-forecast"


class AzureBlobDownload(Download):

    def download_file(
            self,
            url: str,
            account: str,
            container: str,
            key: str,
            blob: None,
            **kwargs: Any,
    ) -> str:
        """Download file from blob storage and store in provided folder or temporary
        folder if no folder supplied.

        Args:
            url (str): URL for the exact blob location
            account (str): Storage account to access the blob
            container (str): Container to download from
            key (str): Key to access the blob
            blob (str): Name of the blob to be downloaded. If empty, then it is assumed to download the whole container.
            **kwargs: See below
            folder (str): Folder to download it to. Defaults to temporary folder.
            filename (str): Filename to use for downloaded file. Defaults to deriving from url.
            path (str): Full path to use for downloaded file instead of folder and filename.
            overwrite (bool): Whether to overwrite existing file. Defaults to False.
            keep (bool): Whether to keep already downloaded file. Defaults to False.
            post (bool): Whether to use POST instead of GET. Defaults to False.
            parameters (Dict): Parameters to pass. Defaults to None.
            timeout (float): Timeout for connecting to URL. Defaults to None (no timeout).
            headers (Dict): Headers to pass. Defaults to None.
            encoding (str): Encoding to use for text response. Defaults to None (best guess).

        Returns:
            str: Path of downloaded file
        """
        folder = kwargs.get("folder")
        filename = kwargs.get("filename")
        path = kwargs.get("path")
        overwrite = kwargs.get("overwrite", False)
        keep = kwargs.get("keep", False)

        request_time = datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
        api_version = '2018-03-28'
        parameters = {
            'verb': 'GET',
            'Content-Encoding': '',
            'Content-Language': '',
            'Content-Length': '',
            'Content-MD5': '',
            'Content-Type': '',
            'Date': '',
            'If-Modified-Since': '',
            'If-Match': '',
            'If-None-Match': '',
            'If-Unmodified-Since': '',
            'Range': '',
            'CanonicalizedHeaders': 'x-ms-date:' + request_time + '\nx-ms-version:' + api_version + '\n',
            'CanonicalizedResource': '/' + account + '/' + container + '/' + blob
        }

        signature = (parameters['verb'] + '\n'
                     + parameters['Content-Encoding'] + '\n'
                     + parameters['Content-Language'] + '\n'
                     + parameters['Content-Length'] + '\n'
                     + parameters['Content-MD5'] + '\n'
                     + parameters['Content-Type'] + '\n'
                     + parameters['Date'] + '\n'
                     + parameters['If-Modified-Since'] + '\n'
                     + parameters['If-Match'] + '\n'
                     + parameters['If-None-Match'] + '\n'
                     + parameters['If-Unmodified-Since'] + '\n'
                     + parameters['Range'] + '\n'
                     + parameters['CanonicalizedHeaders']
                     + parameters['CanonicalizedResource'])

        signed_string = base64.b64encode(hmac.new(base64.b64decode(key), msg=signature.encode('utf-8'),
                                                  digestmod=hashlib.sha256).digest()).decode()

        headers = {
            'x-ms-date': request_time,
            'x-ms-version': api_version,
            'Authorization': ('SharedKey ' + account + ':' + signed_string)
        }

        url = ('https://' + account + '.blob.core.windows.net/' + container + '/' + blob)

        if keep and exists(url):
            print(f"The blob URL exists: {url}")
            return path
        self.setup(
            url=url,
            stream=True,
            post=kwargs.get("post", False),
            parameters=kwargs.get("parameters"),
            timeout=kwargs.get("timeout"),
            headers=headers,
            encoding=kwargs.get("encoding"),
        )
        return self.stream_path(
            path, f"Download of {url} failed in retrieval of stream!"
        )


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and upload them to Azure"""
    with tempfile.TemporaryDirectory() as folder:
        with AzureBlobDownload() as downloader:
            retriever = Retrieve(downloader, folder, "saved_data", folder, save, use_saved)
            configuration = Configuration.read()
            nhc_forecast = NHCHurricaneForecast(configuration, retriever)
            datasets = nhc_forecast.get_data()
            if datasets:
                logger.info(f"Number of datasets to upload: {len(datasets)}")
                try:
                    nhc_forecast.upload_dataset(datasets)
                    logger.info("Successfully uploaded file(s) to blob")
                except Exception:
                    raise Exception("Failed to upload file to blob")
            else:
                logger.info(f"No datasets were uploaded.")


if __name__ == "__main__":
    print()
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup="nhc",
        project_config_yaml=join("config", "project_configuration.yaml"),

    )
