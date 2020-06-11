import time
import subprocess
from sqlalchemy import create_engine
from tqdm import tqdm

# Import custom variables for National Liveability indicator process
from _project_setup import *

if __name__ == '__main__':
    try:
         # simple timer for log file
        start = time.time()
        start_string = time.strftime("%Y%m%d-%H%M%S")
        script = os.path.basename(sys.argv[0])
        service_areas_string = ', '.join([str(x) for x in service_areas])
        task = f'create service areas ({service_areas_string}) for locations in {full_locale} based on road network'
        print(f"Commencing task: {task} at {start_string}")       
        engine = create_engine(f'''postgresql://{db_user}:{db_pwd}@{db_host}/{db}''', use_native_hstore=False) 
        for distance in service_areas:
            print("    - {}m... ".format(distance)),
            table = "nh{}m".format(distance)
            if engine.has_table(table, schema=point_schema):
                print("Aleady exists; skipping.")
            else:
                sql = f'''
                  CREATE TABLE IF NOT EXISTS {point_schema}.{table}
                    ({points_id} {points_id_type} PRIMARY KEY, 
                     area_sqm   double precision,
                     area_sqkm  double precision,
                     area_ha    double precision,
                     geom geometry);  
                  '''
                # create output spatial feature in Postgresql
                engine.execute(sql)
        
        # create allocation of remaining parcels to process grouped by hex in approximately equal size groups
        sql = f'''
              DROP TABLE IF EXISTS {processing_schema}.hex_parcel_nh_remaining;
              CREATE TABLE {processing_schema}.hex_parcel_nh_remaining AS
              WITH remaining_hex_parcels AS (
              SELECT hex, 
                      hex_parcels.count - COALESCE(processed.count,0) AS remaining
               FROM hex_parcels
               LEFT JOIN (SELECT hex_id AS hex, 
                                 COUNT(a.*) 
                          FROM parcel_dwellings a 
                          LEFT JOIN ind_point.nh1600m b 
                          ON a.gnaf_pid = b.gnaf_pid 
                          WHERE b.gnaf_pid IS NOT NULL
                          GROUP BY hex_id) processed USING (hex)
              )
              SELECT hex,remaining, width_bucket(remaining, 0, (SELECT MAX(remaining) FROM remaining_hex_parcels), 6) bin
              FROM   remaining_hex_parcels
              GROUP BY hex, remaining
              ORDER BY bin, hex;
              '''
        engine.execute(sql)
        
        # Select bin ID numbers to be passed to processors for processing 
        # Note that 
        sql = f'''SELECT DISTINCT(bin) FROM {processing_schema}.hex_parcel_nh_remaining; '''
        iteration_list = set([x[0] for x in engine.execute(sql)])
        sql = f'''SELECT SUM(remaining) FROM {processing_schema}.hex_parcel_nh_remaining; '''
        total_remaining = int([x[0] for x in engine.execute(sql)][0])
        commands = [f'python subprocess_service_areas.py {locale} {bin}' for bin in iteration_list]
        pbar = tqdm(total=total_remaining)
        def run_task(task):
            s = subprocess.Popen(task, shell=True, stdout=subprocess.PIPE)
            try:
                pbar.update(int(s.stdout.readline().decode("utf-8")))
            except:
                pass
        processes = [run_task(c) for c in commands]
        for p in procs:
           p.wait()
                
        print("\n  - ensuring all tables are indexed, and contain only unique ids...")
        for distance in service_areas:
            print("    - {}m... ".format(distance))
            table = "nh{}m".format(distance)
            if engine.has_table(table, schema=point_schema):
                # create index and analyse table
                sql = f'''CREATE INDEX IF NOT EXISTS {table}_gix ON ind_point.{table} USING GIST (geom);ANALYZE {table};'''
                engine.execute(sql)
                euclidean_buffer_area_sqm = int(math.pi*distance**2)
                sql =  '''CREATE TABLE ind_point.pedshed_{distance}m AS
                            SELECT gnaf_pid,
                                   {euclidean_buffer_area_sqm} AS euclidean_{distance}m_sqm,
                                   s.area_sqm AS nh{distance}m_sqm,
                                   s.area_sqm / {euclidean_buffer_area_sqm}.0 AS pedshed_{distance}m
                            FROM ind_point.nh{distance}m s;
                       '''
                engine.execute(sql)
        
        # output to completion log    
        script_running_log(script, task, start, locale)
        
    finally:
        engine.dispose()