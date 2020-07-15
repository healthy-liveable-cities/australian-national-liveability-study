# Script:  area_linkage_tables.py
# Purpose: Create ABS and non-ABS linkage tables using 2016 data sourced from ABS
#
#          Parcel address points are already associated with Meshblock in the parcel_dwellings table
#          Further linkage with the abs_linkage table (actually, a reduced version of the existing mb_dwellings)
#          facilitates aggregation to ABS area units such as SA1, SA2, SA3, SA4.
#
#          The non-ABS linkage table associated points with the suburb and LGA in which they are located, according
#          to ABS sourced spatial features.
#
#          Regarding linkage of addresses with non-ABS structures, while the ABS provides some 
#          correspondence tables between areas, (e.g. SA2 2016 to LGA 2016) this will not be as accurate
#          for our purposes as taking an address point location and evaluating the polygon it intersects.
#          There are pitfalls in this approach (e.g. if a point lies exactly on a boundary), however
#          this is par for the course when generalising unique units into aggregate categories 
#          (ie. points to averages, sums or variances within contiguous areas).
# 
# Author:  Carl Higgs
# Date:    20180710

# Import arcpy module

import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import numpy
import time
import psycopg2 
from progressor import progressor
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *


# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create ABS and non-ABS linkage tables using 2016 data sourced from ABS'

# INPUT PARAMETERS
engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                      pwd  = db_pwd,
                                                                      host = db_host,
                                                                      db   = db))

# OUTPUT PROCESS
task = '\nCreate inclusion area geometries... '
print("Commencing task: {} at {}".format(task,time.strftime("%Y%m%d-%H%M%S")))
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()

# Create area tables
print("  - Analysis region tables... "),
for area in analysis_regions:
    area_id = df_regions.loc[area,'id']
    abbrev = df_regions.loc[area,'abbreviation']
    print("     - {} ({})... ".format(area,area_id)),
    if area == 'sa1':
        additional_fields = '''
        string_agg(distinct(ssc_name_2016),',') AS suburb, 
        string_agg(distinct(lga_name_2016), ', ') AS lga,
        '''
    else: 
        additional_fields = ''
    sql = '''  
      -- remove previous legacy table -- now redundant
      DROP TABLE IF EXISTS area_{abbrev};
      DROP TABLE IF EXISTS area_{abbrev}_included;
      CREATE TABLE area_{abbrev}_included AS
      SELECT {area_id}, 
             {additional_fields}
             SUM(dwelling) AS dwellings,
             SUM(person)   AS persons,
             SUM(area_ha) AS area_ha,
             ST_Union(geom)  AS geom
      FROM area_linkage
      WHERE irsd_score IS NOT NULL
        AND dwelling > 0
        AND urban = 'urban'
        AND study_region IS TRUE
      GROUP BY {area_id}
      ORDER BY {area_id} ASC;
      CREATE INDEX IF NOT EXISTS id_area_{abbrev}_included ON area_{abbrev}_included ({area_id});
      CREATE INDEX IF NOT EXISTS gix_area_{abbrev}_included ON area_{abbrev}_included USING GIST (geom);
      '''.format(area_id = area_id,
                 abbrev = abbrev,
                 additional_fields = additional_fields)
    curs.execute(sql)
    conn.commit()   
    print(" area_{abbrev}_included created.".format(abbrev = abbrev))

print("  - SOS region tables")
create_study_region_tables = '''
  DROP TABLE IF EXISTS study_region_all_sos;
  CREATE TABLE study_region_all_sos AS 
  SELECT sos_name_2016, 
         SUM(dwelling) AS dwelling,
         SUM(person) AS person,
         SUM(area_ha) AS area_ha,
         ST_Union(geom) geom
    FROM area_linkage
    WHERE study_region IS TRUE
    GROUP BY sos_name_2016;
  CREATE UNIQUE INDEX ix_study_region_all_sos ON study_region_all_sos (sos_name_2016);
  CREATE INDEX IF NOT EXISTS gix_study_region_all_sos ON study_region_all_sos USING GIST (geom);
  
  DROP TABLE IF EXISTS study_region_urban;
  CREATE TABLE study_region_urban AS 
  SELECT urban, 
         SUM(dwelling) AS dwelling,
         SUM(person) AS person,
         SUM(area_ha) AS area_ha,
         ST_Union(geom) geom
    FROM area_linkage
    WHERE study_region IS TRUE
    GROUP BY urban;
  CREATE UNIQUE INDEX ix_study_region_urban ON study_region_urban (urban);
  CREATE INDEX IF NOT EXISTS gix_study_region_urban ON study_region_urban USING GIST (geom);
'''.format(region = region.lower(), year = year)
curs.execute(create_study_region_tables)
conn.commit()

if locale!='australia': 
    print("  - SOS indexed by parcel")
    create_parcel_sos = '''
      DROP TABLE IF EXISTS parcel_sos;
      CREATE TABLE parcel_sos AS 
      SELECT a.{id},
             sos_name_2016 
      FROM parcel_dwellings a LEFT JOIN area_linkage b ON a.mb_code_2016 = b.mb_code_2016;
      CREATE UNIQUE INDEX IF NOT EXISTS parcel_sos_idx ON  parcel_sos (gnaf_pid);
      '''.format(id = points_id)
    curs.execute(create_parcel_sos)
    conn.commit()

    print("Make a summary table (if not exists) of parcel points lacking sausage buffer, grouped by section of state (the idea is, only a small proportion should be major or other urban"),
    create_no_sausage_sos_tally = '''
      DROP TABLE IF EXISTS no_sausage_sos_tally;
      CREATE TABLE IF NOT EXISTS no_sausage_sos_tally AS
      SELECT a.sos_name_2016, 
             count(b.*) AS no_sausage_count,
             count(b.*) / (SELECT COUNT(*) FROM parcel_dwellings)::double precision AS no_sausage_prop
      FROM area_linkage a 
      LEFT JOIN no_sausage b ON a.mb_code_2016 = b.mb_code_2016
      GROUP BY sos_name_2016 
      ORDER BY sos_name_2016 DESC;
      DELETE FROM no_sausage_sos_tally WHERE no_sausage_count = 0;
      CREATE UNIQUE INDEX IF NOT EXISTS ix_no_sausage_sos_tally ON no_sausage_sos_tally (sos_name_2016);
     '''
    curs.execute(create_no_sausage_sos_tally)
    conn.commit()
    print("Done.")

    print("Creating summary table  (if not exists) of parcel id and local neighbourhood area... "),
    createTable_nh1600m = '''
      DROP TABLE IF EXISTS nh1600m;
      CREATE TABLE IF NOT EXISTS nh1600m AS
        SELECT {0}, area_sqm, area_sqm/1000000 AS area_sqkm, area_sqm/10000 AS area_ha FROM 
          (SELECT {0}, ST_AREA(geom) AS area_sqm FROM {1}) AS t;
      ALTER TABLE nh1600m ADD PRIMARY KEY ({0});
      '''.format(points_id.lower(),"sausagebuffer_{}".format(distance))
    curs.execute(createTable_nh1600m)
    conn.commit()  
    print("Done.")

# output to completion log    
script_running_log(script, task, start, locale)

# clean up
conn.close()

