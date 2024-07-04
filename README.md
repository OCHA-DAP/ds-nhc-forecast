# NHC Forecast pipeline

The aim of this pipeline is to scrape data from NHC, upload it to our Azure storage and trigger an action from another repo in case there is new data.
The datasets generated here append historical information for observed tracks and forecasted tracks.

### Source: https://www.nhc.noaa.gov/
### Schedule: Every 3h.

## Methodology:
We are scraping the data from `CurrentStorms.json`. 
Expected formats for (full example available [here](https://www.nhc.noaa.gov/productexamples/NHC_JSON_Sample.json):
```
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
                "forecastAdvisory": {
                    "advNum": "011",
                    "issuance": "2023-08-22T03:00:00.000Z",
                    "fileUpdateTime": "2023-08-22T02:55:52.439Z",
                    "url": "https://www.nhc.noaa.gov/text/MIATCPAT1.shtml"
                },...}
```
Expected formats for Forecast Advisory [full example here](https://www.nhc.noaa.gov/productexamples/TCM_example.txt):
```    FORECAST VALID 30/0000Z 22.9N  68.1W
            MAX WIND  85 KT...GUSTS 105 KT.
            64 KT... 15NE  10SE   0SW  10NW.
            50 KT... 30NE  20SE  10SW  20NW.
            34 KT... 80NE  50SE  30SW  60NW.
```
We parse the main `CurrentStorms.json` file and the url listed under the `forecastAdvisory`.
We collected the relevant fields in order to append to two separate historical files containing the following fields:
#### Observed tracks:
`id; name; basin; intensity; pressure; latitude; longitude; lastUpdate`
#### Forecasted tracks:
`id; name; issuance; basin; latitude; longitude; maxwind; validTime`

## Webhook trigger
If there is new data to be appended, this pipeline triggers a Github Action from another [repo](https://github.com/OCHA-DAP/ds-aa-hti-hurricanes).

## How to run pipeline:
Environment variables needed to be configured:
```
    account: {Azure storage account}
    container: {Which Azure container the data should be picked up and uploaded to}
    key: {Token to grant access to the Azure container}
    ghaction_url: {Link to the action to be triggered if there is new data}
    ghp: {Key that grants access to the Github repo that will be triggered}
```
This pipeline is set to be run in our development environment with Github Actions on a schedule, but it can also run locally if all the environment variables are set up and requirements are fulfilled.   





