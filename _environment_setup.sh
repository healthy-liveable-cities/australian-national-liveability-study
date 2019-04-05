# set up analysis environment container, based on OSMnx
docker pull gboeing/osmnx

# set up spatial database container, based on Postgis
docker pull mdillon/postgis

# run postgis server container
docker run --name=postgis -d -e POSTGRES_USER=postgres -e POSTGRES_PASS=huilhuil!42 -e POSTGRES_DBNAME=ind_bangkok  -p 127.0.0.1:5433:5432 -e pg_data:/var/lib/postgresql mdillon/postgis

# run analysis environment from Bash
docker run --rm -it -u 0 --name osmnx --net=host -v %cd%:/home/jovyan/work gboeing/osmnx /bin/bash 

# install additional libraries in analysis environment
### NOTE more work will be rewd to make these persist; perhaps create own docker image 
###  - could customise OSMnx docker file
conda install -y psycopg2
conda install -y xlrd