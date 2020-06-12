import time
import timeit
from subprocess import Popen
from sqlalchemy import create_engine
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm

# Import custom variables for National Liveability indicator process
from _project_setup import *
from script_running_log import script_running_log
from _nh_multiprocessing import create_walkable_neighbourhood_from_bin

if __name__ == '__main__':
    try:
         # simple timer for log file
        start = time.time()
        tic=timeit.default_timer()
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
              SELECT hex,
                     remaining,
                     width_bucket(
                                  remaining,                                             -- value to group
                                  0,                                                     -- minimum bin size
                                  (SELECT MAX(remaining) FROM remaining_hex_parcels), 6  -- maximum bin size
                                  ) bin
              FROM   remaining_hex_parcels
              WHERE remaining_hex_parcels.remaining > 0
              GROUP BY hex, remaining
              ORDER BY bin, hex;
              '''
        engine.execute(sql)
        
        # Select bin ID numbers to be passed to processors for processing 
        sql = f'''SELECT DISTINCT(bin) FROM {processing_schema}.hex_parcel_nh_remaining;'''
        iteration_list = [(locale,x[0]) for x in engine.execute(sql)]
        sql = f'''SELECT SUM(remaining) FROM {processing_schema}.hex_parcel_nh_remaining; '''
        total_total = int([x[0] for x in engine.execute(sql)][0])
        total_processed = 0
        pbar = tqdm(total=total_total, unit="hex", unit_scale=False, desc=f"Processing", leave=False)

        with ProcessPoolExecutor(max_workers=nWorkers) as executor:
            futures = []
            for bin in iteration_list:
                futures.append(executor.submit(create_walkable_neighbourhood_from_bin, bin))
                # try:
            
            for res in as_completed(futures):
                processed = res.result()
                total_processed += processed
                pbar.update(total_processed)

    finally:
        # update allocation of remaining parcels to process grouped by hex in approximately equal size groups
        # There may be some parcels whose service areas cannot be solved - this is okay,
        # and they will be excluded later; however, such instances should be manually investigated to 
        # ensure reasons for not being able to be processed are understood.  In theory, these are outliers
        # and not representative of a systematic failure; we must check to be sure of this.
        # This remainder table is retained as a record of such parcels.
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
              SELECT hex,
                     remaining,
                     width_bucket(
                                  remaining,                                             -- value to group
                                  0,                                                     -- minimum bin size
                                  (SELECT MAX(remaining) FROM remaining_hex_parcels), 6  -- maximum bin size
                                  ) bin
              FROM   remaining_hex_parcels
              WHERE remaining_hex_parcels.remaining > 0
              GROUP BY hex, remaining
              ORDER BY bin, hex;
              '''
        engine.execute(sql)
        print("\n  - ensuring all tables are indexed, and contain only unique ids...")
        for distance in service_areas:
            print("    - {}m... ".format(distance))
            table = "nh{}m".format(distance)
            if engine.has_table(table, schema=point_schema):
                # create index and analyse table
                sql = f'''CREATE INDEX IF NOT EXISTS {table}_gix ON ind_point.{table} USING GIST (geom);
                          ANALYZE ind_point.{table};'''
                engine.execute(sql)
                euclidean_buffer_area_sqm = int(math.pi*distance**2)
                sql = f'''DROP TABLE IF EXISTS ind_point.pedshed_{distance}m;
                          CREATE TABLE ind_point.pedshed_{distance}m AS
                          SELECT gnaf_pid,
                                 {euclidean_buffer_area_sqm} AS euclidean_{distance}m_sqm,
                                 s.area_sqm AS nh{distance}m_sqm,
                                 s.area_sqm / {euclidean_buffer_area_sqm}.0 AS pedshed_{distance}m
                          FROM ind_point.nh{distance}m s;
                       '''
                engine.execute(sql)
        
        # output to completion log    
        script_running_log(script, task, start, locale)
        engine.dispose()
        toc=timeit.default_timer()
        mins = (toc - tic)/60
        print(f"Processing time: {mins:.02f} minutes")