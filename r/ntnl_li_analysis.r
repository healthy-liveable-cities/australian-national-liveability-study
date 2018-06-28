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
# Merge the two result sets
# compare <- merge(od_psma,od_osm, by = c("gnaf_pid","dest"),suffixes = c("_psma","_osm"))
compare <- merge(od_psma,od_osm, by = c("gnaf_pid","dest"),suffixes = c("_psma","_osm"), all = TRUE)

# label the destination factors
compare[, dest:= factor(dest, labels = c("Supermarket","Bus stop"))]

# calculate the difference (psma - osm distance in metres)
compare[,("diff_psma_minus_osm"):= distance_psma - distance_osm, by=1:nrow(compare) ]

# histogram
hist(unlist(compare[, "diff_psma_minus_osm"]))

r.a = round(cor(compare[dest=="Supermarket",c("distance_psma","distance_osm")])[1,2],3)
r.b = round(cor(compare[dest=="Bus stop",c("distance_psma","distance_osm")])[1,2],3)

# summary statistics
compare[,list(min  = min(diff_psma_minus_osm, na.rm = TRUE),
              p2_5  = quantile(diff_psma_minus_osm,0.025, na.rm = TRUE),
              p25  = quantile(diff_psma_minus_osm,0.25, na.rm = TRUE),
              p25  = quantile(diff_psma_minus_osm,0.25, na.rm = TRUE),
              p50  = quantile(diff_psma_minus_osm,0.5, na.rm = TRUE),
              p75  = quantile(diff_psma_minus_osm,0.75, na.rm = TRUE),
              p97_5 = quantile(diff_psma_minus_osm,0.975, na.rm = TRUE),
              max  = max(diff_psma_minus_osm, na.rm = TRUE),
              mean = mean(diff_psma_minus_osm, na.rm = TRUE),
              sd   = sd(diff_psma_minus_osm, na.rm = TRUE)),by=dest]

# plot
p <- ggplot(as.data.frame(compare), aes_string('distance_psma', 'distance_osm')) +
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

# output to csv[
write.csv(compare[dest=="Supermarket",],"../../data/compare_supermarket_osm_psma.csv",row.names=F,quote=F)
write.csv(compare[dest=="Bus stop",],"../../data/compare_busstop_osm_psma.csv",row.names=F,quote=F)

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
isnullx(compare$distance_osm)
isnullx(compare$diff_psma_minus_osm)


