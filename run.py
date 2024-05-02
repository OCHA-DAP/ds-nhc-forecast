#!/usr/bin/python
"""
Top level script. Calls other functions that generate datasets that this script then creates in HDX.

"""
import logging
from copy import deepcopy
from os.path import expanduser, join, exists
from typing import Any

from hdx.api.configuration import Configuration
from hdx.facades.infer_arguments import facade
from hdx.utilities.downloader import Download
from hdx.utilities.errors_onexit import ErrorsOnExit
from hdx.utilities.retriever import Retrieve
from nhc_forecast import NHCHurricaneForecast

logger = logging.getLogger(__name__)


def main(save: bool = False, use_saved: bool = False) -> None:
    """Generate datasets and upload them to Azure"""
    folder ="tmp"
    with ErrorsOnExit() as errors:
        with Download() as downloader:
            retriever = Retrieve(
                downloader, folder, "saved_data", folder, save, use_saved)
            configuration = Configuration.read()
            nhc_forecast = NHCHurricaneForecast(configuration, retriever, folder, errors)
            datasets = nhc_forecast.get_data()
            logger.info(f"Number of datasets to upload: {len(datasets)}")
            try:
                nhc_forecast.upload_dataset(datasets)
                logger.info("Successfully upload file to blob")
            except Exception:
                logger.error("Failed to upload file to blob")


if __name__ == "__main__":
    print()
    facade(
        main,
        user_agent_config_yaml=join(expanduser("~"), ".useragents.yaml"),
        user_agent_lookup="nhc",
        project_config_yaml=join("config", "project_configuration.yaml"),

    )
