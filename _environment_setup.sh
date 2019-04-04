# set up analysis environment container, based on OSMnx
docker pull gboeing/osmnx

# set up spatial database container, based on Postgis
docker pull mdillon/postgis

# create persistant storage volume
docker volume create pg_data

# run postgis server container
docker run --name=postgis -d -e POSTGRES_USER=hlc -e POSTGRES_PASS=huilhuil!42 -e POSTGRES_DBNAME=ind_bangkok -e  ALLOW_IP_RANGE=127.0.0.1 -p 5433:5432 -e pg_data:/var/lib/postgresql mdillon/postgis

# run analysis environment from Bash
docker run --rm -it -u 0 --name osmnx -v %cd%:/home/jovyan/work gboeing/osmnx /bin/bash

# install additional libraries in analysis environment
### NOTE more work will be rewd to make these persist; perhaps create own docker image 
###  - could customise OSMnx docker file
conda install -y -q psycopg2
conda install -y -q xlrd