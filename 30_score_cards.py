# Purpose: Create liveability score cards
# Author:  Carl Higgs 
# Date:    2020-01-13

import time
import psycopg2 
import subprocess as sp     # for executing external commands (e.g. pgsql2shp)
from sqlalchemy import create_engine
from datetime import datetime

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'Create liveability score cards'

date = datetime.today().strftime('%Y%m%d')

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()  

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db))


if locale!='australia':
    out_dir = 'D:/ntnl_li_2018_template/data/study_region/_exports'
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)
    out_file = 'score_card_{}_{}_{}_Fc.sql'.format(locale,year,date)

    print("2018/19 score cards...")
    print('\r\nCreate address level score card...'),
    sql = '''
    DROP TABLE IF EXISTS ind_score_card;
    CREATE TABLE IF NOT EXISTS ind_score_card AS
    SELECT 
        p.{id}                    ,
        p.count_objectid          ,
        p.point_x                 ,
        p.point_y                 ,
        p.hex_id                  ,
        '{full_locale}'::text AS study_region,
        '{locale}'::text AS locale      ,
        area.mb_code_2016         ,
        area.mb_category_name_2016,
        area.sa1_maincode_2016    ,
        area.sa2_name_2016        ,
        area.sa3_name_2016        ,
        area.sa4_name_2016        ,
        area.gccsa_name_2016      ,
        area.state_name_2016      ,
        area.ssc_name_2016        ,
        area.lga_name_2016        ,
        area.ucl_name_2016        ,
        area.sos_name_2016        ,
        area.urban                ,
        area.irsd_score           ,
        e.exclude                 ,
        dl_soft_1600m AS daily_living,
        sc_nh1600m AS street_connectivity,
        dd_nh1600m AS dwelling_density,
        ind_walkability.walkability_index,
        uli.uli AS liveability_index,
        nh_inds_distance.supermarket_hlc_2017_osm_2018 AS closest_supermarket,
        array_min(alcohol_offlicence.distances) AS closest_alcohol_offlicence,
        ind_si_mix.si_mix AS social_infrastructure_mix,
        threshold_hard(nh_inds_distance.gtfs_20191008_20191205_pt_0030,400) AS frequent_pt_400m,
        threshold_hard(ind_os_distance.pos_15k_sqm_distance_m,400) AS large_pos_400m,
        live_sa1_work_sa3.pct_live_work_local_area ,
        abs_ind_30_40.pcent_30_40 as pct_30_40_affordable_housing,
        p.geom              
    FROM     
    parcel_dwellings p                                                                                 
    LEFT JOIN area_linkage area ON p.mb_code_20 = area.mb_code_2016
    LEFT JOIN (SELECT {id}, string_agg(indicator,',') AS exclude 
               FROM excluded_parcels GROUP BY {id}) e 
           ON p.{id} = e.{id}
    LEFT JOIN ind_daily_living ON p.{id} = ind_daily_living.{id}
    LEFT JOIN sc_nh1600m ON p.{id} = sc_nh1600m.{id}
    LEFT JOIN dd_nh1600m ON p.{id} = dd_nh1600m.{id}
    LEFT JOIN uli ON p.{id} = uli.{id}
    LEFT JOIN ind_walkability ON p.{id} = ind_walkability.{id}
    LEFT JOIN d_3200m_cl.activity_centres ON p.{id} = d_3200m_cl.activity_centres.{id}
    LEFT JOIN d_3200m_cl.alcohol_offlicence ON p.{id} = d_3200m_cl.alcohol_offlicence.{id}
    LEFT JOIN ind_si_mix ON p.{id} = ind_si_mix.{id}
    LEFT JOIN nh_inds_distance ON p.{id} = nh_inds_distance.{id}
    LEFT JOIN ind_os_distance ON p.{id} = ind_os_distance.{id}
    LEFT JOIN live_sa1_work_sa3 ON area.sa1_7digitcode_2016 = live_sa1_work_sa3.sa1_7digitcode_2016::text
    LEFT JOIN abs_ind_30_40 ON area.sa1_7digitcode_2016 = abs_ind_30_40.sa1_7digitcode_2016::text;
    CREATE UNIQUE INDEX IF NOT EXISTS ix_score_card ON  ind_score_card ({id});
    CREATE INDEX IF NOT EXISTS gix_score_card ON ind_score_card USING GIST (geom);
    '''.format(id = points_id, 
               full_locale = full_locale,
               locale = locale)
    curs.execute(sql)
    conn.commit()
    print('Done.')

    print('\r\nCreate area level score cards...')
    print('  - Mesh Block')
    sql = '''
    DROP TABLE IF EXISTS ind_score_card_mb_init;
    CREATE TABLE IF NOT EXISTS ind_score_card_mb_init AS
    SELECT a.mb_code_2016          ,
           a.mb_category_name_2016 ,
           t.study_region,
           t.locale,
           a.dwelling              ,
           a.person                ,
           a.sa1_maincode_2016     ,
           a.sa2_name_2016         ,
           a.sa3_name_2016         ,
           a.sa4_name_2016         ,
           a.gccsa_name_2016       ,
           a.state_name_2016       ,
           a.ssc_name_2016         ,
           a.lga_name_2016         ,
           a.ucl_name_2016         ,
           a.sos_name_2016         ,
           a.urban                 ,
           a.irsd_score            ,
           a.area_ha               ,
            daily_living,
            street_connectivity,
            dwelling_density,
            walkability_index,
            liveability_index,
            social_infrastructure_mix,
            closest_supermarket,
            closest_alcohol_offlicence,
            frequent_pt_400m,
            large_pos_400m,
            pct_live_work_local_area, 
            pct_30_40_affordable_housing,
           sample_count                                   ,
           sample_count / a.area_ha AS sample_count_per_ha,
           a.geom                 
    FROM area_linkage a 
    LEFT JOIN (
        SELECT  p.mb_code_2016,
                string_agg(DISTINCT(p.study_region),',')::varchar study_region,
                string_agg(DISTINCT(p.locale),',')::varchar locale,
                COUNT(p.*) AS sample_count       ,
                AVG(daily_living) AS daily_living,
                AVG(street_connectivity) AS street_connectivity,
                AVG(walkability_index) AS walkability_index,
                AVG(dwelling_density) AS dwelling_density,
                AVG(liveability_index) AS liveability_index,
                AVG(closest_supermarket) AS closest_supermarket,
                AVG(closest_alcohol_offlicence) AS closest_alcohol_offlicence,
                AVG(social_infrastructure_mix)  AS social_infrastructure_mix,
                100 * AVG(frequent_pt_400m) AS frequent_pt_400m,
                100 * AVG(large_pos_400m) AS large_pos_400m,
                AVG(pct_live_work_local_area) pct_live_work_local_area ,
                AVG(pct_30_40_affordable_housing) as pct_30_40_affordable_housing
        FROM ind_score_card p
        WHERE p.exclude IS NULL
        GROUP BY p.mb_code_2016) t USING (mb_code_2016)
    WHERE a.irsd_score IS NOT NULL
      AND a.dwelling > 0
      AND a.urban = 'urban'
      AND a.study_region IS TRUE
      AND sample_count > 0
    ;
    CREATE UNIQUE INDEX IF NOT EXISTS ix_area_indicators_mb_json ON  area_indicators_mb_json (mb_code_2016);
    CREATE INDEX IF NOT EXISTS gix_area_indicators_mb_json ON area_indicators_mb_json USING GIST (geom);
    '''
    curs.execute(sql)
    conn.commit()
    print('Done.')

    ind_list = ['daily_living',
            'street_connectivity',
            'dwelling_density',
            'walkability_index',
            'liveability_index',
            'social_infrastructure_mix',
            'closest_supermarket',
            'closest_alcohol_offlicence',
            'frequent_pt_400m',
            'large_pos_400m',
            'pct_live_work_local_area', 
            'pct_30_40_affordable_housing']

    print("Creating weighted area aggregate tables:")
    for area in analysis_regions + ['study region']:   
        if area != 'study region':
            area_id = df_regions.loc[area,'id']
            abbrev = df_regions.loc[area,'abbreviation']
            include_region = 'study_region,'
        else: 
            area_id = 'study_region'
            abbrev  = 'region'
            include_region = ''
        if area != 'Section of State':
            pkey = area_id
        else: 
            pkey = '{},study_region'.format(area_id)
        for standard in ['dwelling','person']:
            print("  - score_card_{}_{}".format(abbrev,standard))
            sql = '''
            DROP TABLE IF EXISTS score_card_{abbrev}_{standard};
            CREATE TABLE score_card_{abbrev}_{standard} AS
            SELECT 
            {area_code},
            {include_region}
            locale,
            SUM(dwelling) AS dwelling,
            SUM(person) AS person,
            SUM(sample_count) AS sample_count,
            SUM(sample_count)/SUM(area_ha) AS sample_count_per_ha,
            SUM(area_ha) AS area_ha,
            {extract},
            ST_Union(geom) AS geom
            FROM ind_score_card_mb_init
            GROUP BY {area_code},study_region,locale;
            '''.format(area_code = area_id,
                       abbrev = abbrev,
                       include_region = include_region,
                       extract = ','.join(['''
                           (CASE             
                                -- if there are no units (dwellings or persons) the indicator is null
                                WHEN COALESCE(SUM({standard}),0) = 0
                                    THEN NULL
                                -- else, calculate the value of the unit weighted indicator
                                ELSE                             
                                   ((SUM({standard}*{i})::numeric)/SUM({standard}))::numeric
                              END) AS "{i}"
                       '''.format(i = i,standard = standard) for i in ind_list]),
                       standard = standard
                       )
            curs.execute(sql)
            conn.commit()
            sql = '''
            ALTER TABLE  score_card_{abbrev}_{standard} ADD PRIMARY KEY ({pkey});
            '''.format(pkey = pkey,
                       abbrev = abbrev,
                       standard = standard)
            curs.execute(sql)
            conn.commit()
        
    print("Creating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    command = (
               'pg_dump -U {db_user} -h localhost -Fc  '
               '-t "score_card_lga_dwelling" -t "score_card_lga_person" -t "score_card_mb_dwelling" '
               '-t "score_card_mb_person" -t "score_card_region_dwelling" -t "score_card_region_person" '
               '-t "score_card_sa1_dwelling" -t "score_card_sa1_person" -t "score_card_sa2_dwelling" '
               '-t "score_card_sa2_person" -t "score_card_sa3_dwelling" -t "score_card_sa3_person" '
               '-t "score_card_sa4_dwelling" -t "score_card_sa4_person" -t "score_card_sos_dwelling" '
               '-t "score_card_sos_person" -t "score_card_ssc_dwelling" -t "score_card_ssc_person" '
               '-t "ind_score_card" '
               '{db} > {out_file}'
               ).format(db = db,db_user = db_user,out_file=out_file)  
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")

# Create table schema definition using Albury Wodonga:
if locale=='albury_wodonga':
    out_file = 'score_cards_schema.sql'.format(db)
    print("Creating sql dump to: {}".format(os.path.join(out_dir,out_file))),
    command = (
               'pg_dump -U {db_user} -h localhost --schema-only '
               '-t "score_card_lga_dwelling" -t "score_card_lga_person" -t "score_card_mb_dwelling" '
               '-t "score_card_mb_person" -t "score_card_region_dwelling" -t "score_card_region_person" '
               '-t "score_card_sa1_dwelling" -t "score_card_sa1_person" -t "score_card_sa2_dwelling" '
               '-t "score_card_sa2_person" -t "score_card_sa3_dwelling" -t "score_card_sa3_person" '
               '-t "score_card_sa4_dwelling" -t "score_card_sa4_person" -t "score_card_sos_dwelling" '
               '-t "score_card_sos_person" -t "score_card_ssc_dwelling" -t "score_card_ssc_person" '
               '-t "ind_score_card" '
               '{db} > {out_file}'
               ).format(db = db,db_user = db_user,out_file=out_file)    
    sp.call(command, shell=True,cwd=out_dir)   
    print("Done.")

if locale=='australia':
    # Connect to postgresql database     
    db = 'li_australia_2018'
    year = 2018
    exports_dir = os.path.join(folderPath,'study_region','_exports')
    print("This script assumes the database {db} has been created!\n".format(db = db))
    conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
    curs = conn.cursor()
    sql = '''
    DROP TABLE IF EXISTS score_card_lga_dwelling    ;
    DROP TABLE IF EXISTS score_card_lga_person      ;
    DROP TABLE IF EXISTS score_card_mb_dwelling     ;
    DROP TABLE IF EXISTS score_card_mb_person       ;
    DROP TABLE IF EXISTS score_card_region_dwelling ;
    DROP TABLE IF EXISTS score_card_region_person   ;
    DROP TABLE IF EXISTS score_card_sa1_dwelling    ;
    DROP TABLE IF EXISTS score_card_sa1_person      ;
    DROP TABLE IF EXISTS score_card_sa2_dwelling    ;
    DROP TABLE IF EXISTS score_card_sa2_person      ;
    DROP TABLE IF EXISTS score_card_sa3_dwelling    ;
    DROP TABLE IF EXISTS score_card_sa3_person      ;
    DROP TABLE IF EXISTS score_card_sa4_dwelling    ;
    DROP TABLE IF EXISTS score_card_sa4_person      ;
    DROP TABLE IF EXISTS score_card_sos_dwelling    ;
    DROP TABLE IF EXISTS score_card_sos_person      ;
    DROP TABLE IF EXISTS score_card_ssc_dwelling    ;
    DROP TABLE IF EXISTS score_card_ssc_person      ;
    DROP TABLE IF EXISTS ind_score_card;
    '''
    curs.execute(sql)
    conn.commit()

    print("Create empty tables for parcel indicators... ")
    command = 'psql li_australia_2018 < score_cards_schema.sql'
    sp.call(command, shell=True,cwd=exports_dir)   

    # curs.execute('''SELECT study_region FROM score_card_region_dwelling;''')
    # processed_locales = [x[0] for x in curs.fetchall()]
    processed_locales = []
    print("Done.\n")

    print("Looping over study regions and importing data if available and not previously processed...")
    locale_field_length = 7 + len(max(study_regions,key=len))
    for locale in sorted(study_regions, key=str.lower):
      sql = 'score_card_{}_{}_20200113_Fc.sql'.format(locale,year)
      if locale in processed_locales:
        print((" - {:"+str(locale_field_length)+"}: previously processed").format(locale))
      elif os.path.isfile(os.path.join(exports_dir,sql)):
        print((" - {:"+str(locale_field_length)+"}: processing now... ").format(locale)),
        command = 'pg_restore -a -Fc -d li_australia_2018 < {}'.format(sql)
        sp.call(command, shell=True,cwd=exports_dir)   
        print("Done!")
      else:
        print((" - {:"+str(locale_field_length)+"}: data apparently not available ").format(locale))


print("All done!")
# output to completion log    
script_running_log(script, task, start)
