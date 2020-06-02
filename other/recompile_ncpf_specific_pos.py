# Script:  recompile_ncpf_specific_pos.py
# Purpose: This script recompiles POS nodes from aos nodes for the NCPF indicators
#
# Author:  Carl Higgs
# Date:    20190107

import arcpy
import time
import psycopg2

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# Create destination type table in sql database
# connect to the PostgreSQL server
conn = psycopg2.connect(dbname=db, user=db_user, password=db_pwd)
curs = conn.cursor()

print("Create any pos nodes... "),
create_pos_nodes = '''
-- any pos nodes
DROP TABLE IF EXISTS aos_nodes_30m_pos_any;
CREATE TABLE aos_nodes_30m_pos_any AS
SELECT n.* 
  FROM aos_nodes_30m_line n
  LEFT JOIN open_space_areas o ON n.aos_id = o.aos_id
 WHERE o.aos_ha_public > 0;
 
 -- any pos indices
DROP INDEX IF EXISTS idx_aos_nodes_30m_pos_any;
DROP INDEX IF EXISTS idx_aos_nodes_30m_pos_any_geom;
CREATE INDEX idx_aos_nodes_30m_pos_any ON aos_nodes_30m_pos_any (aos_entryid);
CREATE INDEX idx_aos_nodes_30m_pos_any_geom ON aos_nodes_30m_pos_any USING GIST (geom);
'''
curs.execute(create_pos_nodes)
conn.commit()
print("Done.")

print("Create large pos nodes... "),
create_pos_nodes = '''
--large pos nodes
DROP TABLE IF EXISTS aos_nodes_30m_pos_large;
CREATE TABLE aos_nodes_30m_pos_large AS
SELECT n.* 
  FROM aos_nodes_30m_line n
  LEFT JOIN open_space_areas o ON n.aos_id = o.aos_id
 WHERE o.aos_ha_public > 1.5;
 
-- large pos indices
DROP INDEX IF EXISTS idx_aos_nodes_30m_pos_large;
DROP INDEX IF EXISTS idx_aos_nodes_30m_pos_large_geom;
CREATE INDEX idx_aos_nodes_30m_pos_large ON aos_nodes_30m_pos_large (aos_entryid);
CREATE INDEX idx_aos_nodes_30m_pos_large_geom ON aos_nodes_30m_pos_large USING GIST (geom);
 '''
curs.execute(create_pos_nodes)
conn.commit()
print("Done.")

# Copy study region destination table from PostgreSQL db to ArcGIS gdb
print("Copy pos nodes to ArcGIS gdb... "),
curs.execute(grant_query)
conn.commit()
arcpy.env.workspace = db_sde_path
arcpy.env.overwriteOutput = True 
arcpy.CopyFeatures_management('public.aos_nodes_30m_pos_any', os.path.join(gdb_path,'aos_nodes_30m_pos_any')) 
arcpy.CopyFeatures_management('public.aos_nodes_30m_pos_large', os.path.join(gdb_path,'aos_nodes_30m_pos_large')) 
print("Done.") 
