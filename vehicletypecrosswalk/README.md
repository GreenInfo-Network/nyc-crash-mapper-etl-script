This utility program examines the vehicle type crosswalk table, and compares to the actual set of `vehicle_type` values found in the crashes table. It then reports various anomalies and mismatches for potential manual repairs.

Short summary:
* `crashes_all_prod` DB table, `vehicle_type` field is an array of text values received from our data source
* `vehicletype_crosswalk_prod` DB table provides a remapping of these values onto a set of 8 domain values: CAR, TRUCK, etc.
* These remapped values are used to set `hasvehicle_XXX` fields, which are used in filtering and reporting.

For more information see https://github.com/GreenInfo-Network/nyc-crash-mapper-etl-script/issues/22

Hundreds of different free-form text values, typographical variants, and just plain misspellings have been noted and entered into this table for remapping. Still, it is assumed that new variants may crop up at any time, and that those future crashes may not be tagged appropriately. This tool will bring these situations to light for manual repair as needed.


## Setup

Set the following environment variabes in your shell. Look up the values from the Heroku panel and copy them in.

```
export CARTO_API_KEY='<redacted>'
```

You may find it useful to create a fil called `.env` which contais these commands, then to use `source .env` to load those variables into your shell.

Install Python requirements:

```
pip install -r requirements.txt
```


## Running the Tool

Run the script using Python 2.7 by doing:

```
python check_vehicletypes.py
```
