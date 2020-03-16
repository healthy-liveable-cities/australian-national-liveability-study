import subprocess as sp     # for executing external commands (e.g. pgsql2shp or ogr2ogr)
import time
import psycopg2
from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Prepare custom data sources for high life study'

# connect to the PostgreSQL server and ensure privileges are granted for all public tables
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor() 

sql_queries = ['''
    -- Collate off road paths based on OSM queries
    -- informed by Australian tagging guidelines 
    -- and checks using OSM taginfo and OpenStreetMap in Melbourne
    -- https://wiki.openstreetmap.org/wiki/Australian_Tagging_Guidelines
    DROP TABLE IF EXISTS network.paths_offroad;
    CREATE TABLE network.paths_offroad AS 
    SELECT * FROM
    (SELECT * FROM  osm.osm_20190902_line
    UNION 
    SELECT * FROM osm.osm_20190902_roads
    ) t
     WHERE 
          (
           highway='path'
           OR
           ((highway='track') AND (foot='yes' OR bicycle='yes'))
            OR
           highway='cycleway'
           )
           -- one way cycleways are associated with roads, so exclude
           AND (oneway!='yes' OR oneway IS NULL)
           ;
    CREATE INDEX paths_offroad_gix ON network.paths_offroad USING GIST(geom);
    ''',
    '''
    -- Connected off road paths data set (contiguous path must be > 200m in length)
    DROP TABLE IF EXISTS network.paths_offroad_contiguous;
    CREATE TABLE network.paths_offroad_contiguous AS
    SELECT ROW_NUMBER() OVER () AS path_id, 
           ST_Length(geom)::int AS length,
           geom
    FROM (SELECT        
          ST_UnaryUnion(unnest(ST_ClusterIntersecting(geom))) geom
    FROM network.paths_offroad) t
    -- limit to paths of over 200m length, so we know these are meaningful
    -- off road paths, not just an incidental cut through
    WHERE ST_Length(geom) > 200;
    ''',
    '''
    -- Generate access points every 20m along path outlines, 
    -- retaining those within 30m of the walkable network'
    DROP TABLE IF EXISTS destinations.off_road_path_access_points; 
    CREATE TABLE destinations.off_road_path_access_points AS 
     WITH 
     segments AS 
     -- split geometries into seperate line strings
     (SELECT path_id,
             (ST_Dump(geom)).geom
      FROM network.paths_offroad_contiguous
     ),
     path AS 
     -- create records for series of intervals required along each line
     (SELECT path_id, 
             generate_series(0,1,20/ST_length(geom)::numeric) AS fraction, 
             geom FROM segments),
    off_road_path_access_points AS
    -- create path entry points at regular intervals
    (SELECT path_id,
           row_number() over(PARTITION BY path_id) AS node, 
           ST_LineInterpolatePoint(path.geom, fraction)  AS geom 
    FROM path)
    SELECT DISTINCT n.* 
    FROM off_road_path_access_points n,
          network.edges l
    WHERE ST_DWithin(n.geom ,l.geom,30);

    CREATE INDEX off_road_path_access_points_idx ON destinations.off_road_path_access_points USING GIST (geom);
    ALTER TABLE destinations.off_road_path_access_points ADD COLUMN path_entryid varchar; 
    UPDATE destinations.off_road_path_access_points SET path_entryid = path_id::text || ',' || node::text; 
    ''']

for sql in sql_queries:
    start = time.time()
    print("\nExecuting: {}".format(sql))
    curs.execute(sql)
    conn.commit()
    print("Executed in {} mins".format((time.time()-start)/60))
    
# output to completion log    
script_running_log(script, task, start, locale)
conn.close()