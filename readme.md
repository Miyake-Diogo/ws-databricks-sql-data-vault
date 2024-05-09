# Data Vault [2]: Databricks SQL como Query Engine de Data Lakehouse para Entrega de um Data Vault e OBTs

### set .env file
```shell
export PAYMENTS_FILES='src/objects/payments.csv'
export RIDES_FILES='src/objects/rides.csv'
export VEHICLE_FILES='src/objects/vehicle.csv'

export BLOB_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=??;AccountKey=??==;EndpointSuffix=core.windows.net"
export LANDING_CONTAINER_NAME='owshq-stg-files'
```

### install requirements & azure cli login
```sh
pip install -r requirements.txt
az login
```

### generate data & upload to blob storage
```sh
python cli.py --help
python cli.py all
```