# R script to simulate power for difference in correlations 
# (Pearson, Spearman, and later ... ICC)
# Carl Higgs 2017

# may have to set working directory
setwd("D:\ntnl_li_2018_template\data")

# import required libraries (installing them, if necessary)
# install.packages("data.table")

require(data.table) # Results are stored using data.table
# The code assumes you have 
#   - installed and set up PostgreSQL (the present analysis used version 9.6)
#   - create a database to store tables of results
#       - using psql you can run a query such as: "CREATE DATABASE corrx_twins;" 
#   - set up a config.yml containing connection details for sql database (see config.yml.README.txt for template)
#   - installed the 'RPostgres' package, which is used to interface R and PostgreSQL
# install.packages('RPostgres')
require(DBI)      # used to connext to Postgresql using RPostgres


### Get database od matrix results
# Open Postgres connection
pg.RPostgres <- dbConnect(RPostgres::Postgres(), 
                          dbname   = config::get("sql")$connection$melbourne_psma,
                          host     = config::get("sql")$connection$host,
                          port     = config::get("sql")$connection$port,
                          user     = config::get("sql")$connection$user,
                          password = config::get("sql")$connection$password)
# Fetch PSMA results
res <- dbSendQuery(pg.RPostgres, "SELECT * FROM od_distances")
od_psma <- dbFetch(res)

# clean up and close connection
dbClearResult(res)
dbDisconnect(pg.RPostgres)

# Open Postgres connection
pg.RPostgres <- dbConnect(RPostgres::Postgres(), 
                          dbname   = config::get("sql")$connection$melbourne,
                          host     = config::get("sql")$connection$host,
                          port     = config::get("sql")$connection$port,
                          user     = config::get("sql")$connection$user,
                          password = config::get("sql")$connection$password)
# Fetch OSM results
res <- dbSendQuery(pg.RPostgres, "SELECT * FROM od_distances")
od_psma <- dbFetch(res)

# clean up and close connection
dbClearResult(res)
dbDisconnect(pg.RPostgres)

