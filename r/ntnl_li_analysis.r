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
require(ggplot2)
require(ggExtra)

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
                          dbname   = config::get("sql")$connection$melbourne_osmnx_v1,
                          host     = config::get("sql")$connection$host,
                          port     = config::get("sql")$connection$port,
                          user     = config::get("sql")$connection$user,
                          password = config::get("sql")$connection$password)
# Fetch OSM results
res <- dbSendQuery(pg.RPostgres, "SELECT * FROM od_distances")
od_osmnx_v1 <- data.table(dbFetch(res))

# clean up and close connection
dbClearResult(res)
dbDisconnect(pg.RPostgres)

# Open Postgres connection
pg.RPostgres <- dbConnect(RPostgres::Postgres(), 
                          dbname   = config::get("sql")$connection$melbourne_osmnx_v2,
                          host     = config::get("sql")$connection$host,
                          port     = config::get("sql")$connection$port,
                          user     = config::get("sql")$connection$user,
                          password = config::get("sql")$connection$password)
# Fetch OSM results
res <- dbSendQuery(pg.RPostgres, "SELECT * FROM od_distances")
od_osmnx_v2 <- data.table(dbFetch(res))

# clean up and close connection
dbClearResult(res)
dbDisconnect(pg.RPostgres)

od_osm <- fread("D:/ntnl_li_2018_template/data/Melb_OD_RR_20180728.csv",sep=",")

# Merge the two result sets
compare <- merge(od_psma,od_osmnx_v1, by = c("gnaf_pid","dest"),suffixes = c("_psma","_osmnx_v1"), all = TRUE)
compare <- merge(compare,od_osm, by = c("gnaf_pid","dest"),suffixes = c("","_osm"), all = TRUE)
compare <- merge(compare,od_osmnx_v2, by = c("gnaf_pid","dest"),suffixes = c("_psma","_osmnx_v2"), all = TRUE)
colnames(compare) <- c(colnames(compare)[1:6],"oid_osm","distance_osm","oid_osm_v2","distance_osmnx_v2")
compare

# label the destination factors
compare[, dest:= factor(dest, labels = c("Supermarket","Bus stop"))]
# calculate the difference (psma - osm distance in metres)
compare[,("diff_psma_minus_osmnx_v1"):= distance_psma - distance_osmnx_v1, by=1:nrow(compare) ]
compare[,("diff_psma_minus_osm"):=      distance_psma - distance_osm,      by=1:nrow(compare) ]
compare[,("diff_psma_minus_osmnx_v2"):= distance_psma - distance_osmnx_v2, by=1:nrow(compare) ]

# histogram
hist(unlist(compare[, "diff_psma_minus_osmnx_v1"]))
hist(unlist(compare[, "diff_psma_minus_osm"]))
hist(unlist(compare[, "diff_psma_minus_osmnx_v2"]))

r.a = round(cor(compare[dest=="Supermarket",c("distance_psma","distance_osmnx_v1")])[1,2],3)
r.b = round(cor(compare[dest=="Bus stop",c("distance_psma","distance_osmnx_v1")])[1,2],3)

# summary statistics
# OSMnx v1
compare[,list(min   = min(diff_psma_minus_osmnx_v1,            na.rm = TRUE),
              p2_5  = quantile(diff_psma_minus_osmnx_v1,0.025, na.rm = TRUE),
              p25   = quantile(diff_psma_minus_osmnx_v1,0.25,  na.rm = TRUE),
              p50   = quantile(diff_psma_minus_osmnx_v1,0.5,   na.rm = TRUE),
              p75   = quantile(diff_psma_minus_osmnx_v1,0.75,  na.rm = TRUE),
              p97_5 = quantile(diff_psma_minus_osmnx_v1,0.975, na.rm = TRUE),
              max   = max(diff_psma_minus_osmnx_v1,            na.rm = TRUE),
              mean  = mean(diff_psma_minus_osmnx_v1,           na.rm = TRUE),
              sd    = sd(diff_psma_minus_osmnx_v1,             na.rm = TRUE)),
        by=dest]

# In house OSM
compare[,list(min   = min(diff_psma_minus_osm,            na.rm = TRUE),
              p2_5  = quantile(diff_psma_minus_osm,0.025, na.rm = TRUE),
              p25   = quantile(diff_psma_minus_osm,0.25,  na.rm = TRUE),
              p50   = quantile(diff_psma_minus_osm,0.5,   na.rm = TRUE),
              p75   = quantile(diff_psma_minus_osm,0.75,  na.rm = TRUE),
              p97_5 = quantile(diff_psma_minus_osm,0.975, na.rm = TRUE),
              max   = max(diff_psma_minus_osm,            na.rm = TRUE),
              mean  = mean(diff_psma_minus_osm,           na.rm = TRUE),
              sd    = sd(diff_psma_minus_osm,             na.rm = TRUE)),
        by=dest]

# OSMnx v2
compare[,list(min   = min(diff_psma_minus_osmnx_v2,            na.rm = TRUE),
              p2_5  = quantile(diff_psma_minus_osmnx_v2,0.025, na.rm = TRUE),
              p25   = quantile(diff_psma_minus_osmnx_v2,0.25,  na.rm = TRUE),
              p50   = quantile(diff_psma_minus_osmnx_v2,0.5,   na.rm = TRUE),
              p75   = quantile(diff_psma_minus_osmnx_v2,0.75,  na.rm = TRUE),
              p97_5 = quantile(diff_psma_minus_osmnx_v2,0.975, na.rm = TRUE),
              max   = max(diff_psma_minus_osmnx_v2,            na.rm = TRUE),
              mean  = mean(diff_psma_minus_osmnx_v2,           na.rm = TRUE),
              sd    = sd(diff_psma_minus_osmnx_v2,             na.rm = TRUE)),
        by=dest]

# plot
p <- ggplot(as.data.frame(compare), aes_string('distance_psma', 'distance_osmnx_v1')) +
  aes_string(colour = 'dest') +
  geom_point() + theme_bw(15) +
  theme(legend.position = c(1.04, 1.1),legend.text.align	 = 0)  +
  scale_color_manual(labels = c(bquote(paste("Supermarket (r = ",.(r.a),")")), 
                                bquote(paste("Bus stops   (r = ",.(r.b),")"))),
                     values = c("Supermarket" = "#ef8a62","Bus stop" = "#67a9cf")) 
p <- ggMarginal(p,
                type = 'density',
                margins = 'both',
                size = 5,
                groupColour = TRUE,
                groupFill = TRUE,
                alpha = 0.4
)

print(p)

# plot
p2 <- ggplot(as.data.frame(compare), aes_string('diff_psma_minus_osmnx_v1', 'distance_osmnx_v1')) +
  aes_string(colour = 'dest') +
  geom_point() + theme_bw(15) +
  theme(legend.position = c(1.04, 1.1),legend.text.align	 = 0)  +
  scale_color_manual(values = c("Supermarket" = "#ef8a62","Bus stop" = "#67a9cf")) 
p2 <- ggMarginal(p2,
                 type = 'density',
                 margins = 'both',
                 size = 5,
                 groupColour = TRUE,
                 groupFill = TRUE,
                 alpha = 0.4
)

print(p2)



# output to csv[
write.csv(compare[dest=="Supermarket",],"../../data/compare_supermarket_osmnx_v1_psma.csv",row.names=F,quote=F)
write.csv(compare[dest=="Bus stop",],"../../data/compare_busstop_osmnx_v1_psma.csv",row.names=F,quote=F)

isnullx <- function(x) {
  nulltable <- as.vector(table(is.na(x)))
  summary <- cbind(Null = nulltable[2], 
                   Result = nulltable[1], 
                   Total = sum(nulltable), 
                   Percent_null = nulltable[2]/sum(nulltable)*100)
  # print(summary)
  return(summary)
}

isnullx(compare$distance_psma)
isnullx(compare$distance_osmnx_v1)
isnullx(compare$diff_psma_minus_osmnx_v1)


isnullx(compare[dest=="Supermarket",distance_psma])
isnullx(compare[dest=="Supermarket",distance_osmnx_v1])
isnullx(compare[dest=="Supermarket",diff_psma_minus_osmnx_v1])

isnullx(compare[dest=="Bus stop",distance_psma])
isnullx(compare[dest=="Bus stop",distance_osmnx_v1])
isnullx(compare[dest=="Bus stop",diff_psma_minus_osmnx_v1])

compare[abs(diff_psma_minus_osmnx_v1) > 10000,]
compare[abs(diff_psma_minus_osm) > 10000,]


table(compare[dest=="Supermarket",diff_psma_minus_osmnx_v1  < -300,])
# FALSE    TRUE 
# 1724337   61257 

table(compare[dest=="Supermarket",diff_psma_minus_osm  < -300,])
# FALSE    TRUE 
# 1665520  117297 

table(compare[dest=="Supermarket",diff_psma_minus_osmnx_v2 < -300,])
# FALSE    TRUE 
# 1729660   55934 

table(compare[dest=="Supermarket",diff_psma_minus_osmnx_v1  < -500,])
# FALSE    TRUE 
# 1757561   28033 

table(compare[dest=="Supermarket",diff_psma_minus_osm  < -500,])
# FALSE    TRUE 
# 1698702   84115 

table(compare[dest=="Supermarket",diff_psma_minus_osmnx_v2 < -500,])
# FALSE    TRUE 
# 1760459   25135 

table(compare[dest=="Supermarket",diff_psma_minus_osmnx_v1  < -1000,])
# FALSE    TRUE 
# 1778100    7494 
# 
table(compare[dest=="Supermarket",diff_psma_minus_osm  < -1000,])
# FALSE    TRUE 
# 1730139   52678 

table(compare[dest=="Supermarket",diff_psma_minus_osmnx_v2 < -1000,])
# FALSE    TRUE 
# 1778472    7122 

table(compare[dest=="Supermarket",diff_psma_minus_osmnx_v1  < -10000,])
# FALSE    TRUE 
# 1785585       9 

table(compare[dest=="Supermarket",diff_psma_minus_osm  < -10000,])
# FALSE 
# 1782817 

table(compare[dest=="Supermarket",diff_psma_minus_osmnx_v2 < -10000,])
# FALSE    TRUE 
# 1785585       9 
