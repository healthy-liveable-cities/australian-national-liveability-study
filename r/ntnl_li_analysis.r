# R script to simulate power for difference in correlations 
# (Pearson, Spearman, and later ... ICC)
# Carl Higgs 2017

# may have to set working directory
setwd("D:/ntnl_li_2018_template/process/r")

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
od_psma <- data.table(dbFetch(res))

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
od_osm <- data.table(dbFetch(res))

# clean up and close connection
dbClearResult(res)
dbDisconnect(pg.RPostgres)

# Merge the two result sets
compare <- merge(od_osm, od_psma, by = c("gnaf_pid","dest"),suffixes = c("_osm","_psma"))

# label the destination factors
compare[, dest:= factor(dest, labels = c("Supermarket","Bus stop"))]

# calculate the difference (psma - osm distance in metres)
compare[,("diff_psma_minus_osm"):= distance_psma - distance_osm, by=1:nrow(compare) ]

# histogram
hist(unlist(compare[, "diff_psma_minus_osm"]))

# summary statistics
compare[,summary(diff_psma_minus_osm),by=dest]

# output to csv
write.csv(compare,"compare_osm_psma.csv",row.names=F,quote=F)
