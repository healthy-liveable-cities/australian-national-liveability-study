# Script:  ABS_indicators.py
# Purpose: Process ABS indicators (e.g. affordable housing; live work same area)
# Author:  Carl Higgs
# Date:    2020-01-13

#import packages
import os
import sys
import time
import numpy as np
import psycopg2 
from sqlalchemy import create_engine

from script_running_log import script_running_log

# Import custom variables for National Liveability indicator process
from _project_setup import *

# simple timer for log file
start = time.time()
script = os.path.basename(sys.argv[0])
task = 'create destination indicator tables'

conn = psycopg2.connect(database=db, user=db_user, password=db_pwd)
curs = conn.cursor()

engine = create_engine("postgresql://{user}:{pwd}@{host}/{db}".format(user = db_user,
                                                                 pwd  = db_pwd,
                                                                 host = db_host,
                                                                 db   = db), 
                       use_native_hstore=False)

area_codes  = pandas.read_sql('SELECT DISTINCT(sa1_7digitcode_2016)::int FROM area_linkage',engine)


# import affordable housing indicator
print("Affordable housing indicator... "),
affordable_housing = pandas.read_csv('../data/ABS/derived/abs_2016_sa1_housing_3040_20190712.csv', index_col=0)
affordable_housing = affordable_housing.loc[area_codes['sa1_7digitcode_2016']]
affordable_housing.to_sql('abs_ind_30_40', con=engine, schema = 'ind_sa1', if_exists='replace')
print("Done.")

# import mode of transport to work data
print("Mode of transport to work indicators... "),
mtwp = pandas.read_csv('../data/ABS/derived/abs_2016_sa1_mtwp_cleaned.csv', index_col=0)
mtwp = mtwp.loc[area_codes['sa1_7digitcode_2016']]
modes = ['active_transport','public_transport','vehicle','other_mode']
mtwp['total_employed_travelling'] = mtwp[modes].sum(axis=1)
for mode in modes[:-1]:
  # percentage of employed persons travelling to work using this mode
  mtwp['pct_{}'.format(mode)] = 100*mtwp[mode]/mtwp['total_employed_travelling']

mtwp.to_sql('abs_mode_of_transport_to_work', schema='ind_sa1', con=engine, if_exists='replace')
print("Done.")

# import proportion renting indicator
print("Proportion renting indicator... "),
pct_renting = pandas.read_csv('../data/ABS/derived/abs_au_2016_tenure_type_by_sa1.csv', index_col=0)
pct_renting = pct_renting.loc[area_codes['sa1_7digitcode_2016']]
pct_renting['valid_total'] = pct_renting['Total'] - pct_renting['Not stated'] - pct_renting['Not applicable']
pct_renting['pct_renting'] = 100*pct_renting['Rented']/pct_renting['valid_total']
pct_renting.to_sql('abs_pct_renting', con=engine, schema='ind_sa1', if_exists='replace')
print("Done.")


print("Live and work in same local area indicator... "),
indexName = 'sa1_7digitcode_2016'
df = pandas.read_csv('../data/ABS/derived/SA1 (UR) by SA3 (POW) - cleaned_20190712.csv', index_col=0)
df = df.reset_index()
df = pandas.melt(df, id_vars=['sa1_7digitcode_2016'], value_vars=[x for x in df.columns if x!='sa1_7digitcode_2016'],var_name='sa3_work',value_name='count')
df = df.astype(np.int64)

# Get SA1 to SA3 look up table
sql = '''
SELECT DISTINCT(sa1_7digitcode_2016),
       sa3_code_2016 AS sa3_live 
  FROM boundaries.sa1_2016_aust 
GROUP BY sa1_7digitcode_2016, 
         sa3_live 
ORDER BY sa1_7digitcode_2016;
'''
curs.execute(sql)
area_lookup = pandas.DataFrame(curs.fetchall(), columns=['sa1_7digitcode_2016', 'sa3_live'])
area_lookup = area_lookup.astype(np.int64)

# Merge lookup with usual residence (UR) by place of work (POW)
live_work = pandas.merge(df, area_lookup, how='left', left_on='sa1_7digitcode_2016', right_on='sa1_7digitcode_2016')

# remove those areas where no one works
live_work = live_work[live_work['count']!=0]
live_work['local'] = live_work.apply(lambda x: x.sa3_live==x.sa3_work,axis=1)
live_work= live_work.groupby(['sa1_7digitcode_2016','local'])['count'].agg(['sum']).unstack(fill_value = np.nan)
live_work.columns = live_work.columns.droplevel()
live_work = live_work.reset_index()
live_work = live_work.set_index('sa1_7digitcode_2016')

# filter down to area codes in region of interest
local_live_work = live_work.loc[area_codes['sa1_7digitcode_2016']]

live_work = live_work.fillna(0)
live_work = live_work.astype(np.int64)
live_work['total'] = live_work.apply(lambda x: x[False]+x[True],axis=1)
live_work['pct_live_work_local_area'] = live_work.apply(lambda x: 100*(x[True]/float(x['total'])),axis=1)

live_work.to_sql('live_sa1_work_sa3', con=engine, schema='ind_sa1',if_exists='replace')
print("Done.")


print("Compile ABS indicators at SA1 level... "),
dfs = [affordable_housing[['pct_30_40_housing']],
       mtwp[['pct_active_transport','pct_public_transport','pct_vehicle']],
       pct_renting[['pct_renting']],
       live_work[['pct_live_work_local_area']]
       ]
abs_indicators = dfs[0].join(dfs[1:])

abs_indicators.to_sql('abs_indicators',engine, schema='ind_sa1',if_exists='replace')

print("Done.")

print("Create net and gross dwelling density area level indicators... ")
# We drop these tables first, since some destinations may have been processed since previously running.
# These queries are quick to run, so not much cost to drop and create again.
for area in analysis_regions + ['study region']:   
  print("{}... ".format(area)),
  if area != 'study region':
      area_id = df_regions.loc[area,'id']
      abbrev = df_regions.loc[area,'abbreviation']
      include_region = 'study_region,'
      query = '''    
        -- Gross density indicator, using all Mesh Blocks as denominator
        DROP TABLE IF EXISTS ind_{abbrev}.abs_density_gross_{abbrev};
        CREATE TABLE ind_{abbrev}.abs_density_gross_{abbrev} AS
        SELECT overall.{area_id},
               dwelling,
               area,
               dwelling/NULLIF(area,0) AS gross_density,
               urban_dwelling,
               urban_area,
               urban_dwelling/NULLIF(urban_area,0) AS urban_gross_density,
               not_urban_dwelling,
               not_urban_area,
               not_urban_dwelling /NULLIF(not_urban_area,0) AS not_urban_gross_density
        FROM 
        (SELECT {area_id}, 
               COALESCE(SUM(dwelling),0) dwelling , 
               COALESCE(SUM(area_ha),0) area
        FROM area_linkage
        GROUP BY {area_id}) AS overall
        LEFT JOIN 
        (SELECT {area_id}, 
                COALESCE(SUM(dwelling),0) urban_dwelling, 
                COALESCE(SUM(area_ha),0)  urban_area
        FROM area_linkage
        WHERE urban = 'urban' 
        GROUP BY {area_id}) AS urban ON overall.{area_id} = urban.{area_id}
        LEFT JOIN 
        (SELECT {area_id}, 
                COALESCE(SUM(dwelling),0) not_urban_dwelling, 
                COALESCE(SUM(area_ha),0)  not_urban_area
        FROM area_linkage
        WHERE urban = 'not urban' 
        GROUP BY {area_id}) AS not_urban ON overall.{area_id} = not_urban.{area_id};
        CREATE INDEX abs_density_gross_{abbrev}_ix ON ind_{abbrev}.abs_density_gross_{abbrev} ({area_id});
        
        -- Net density indicator, using residential Mesh Blocks as denominator
        DROP TABLE IF EXISTS ind_{abbrev}.abs_density_net_{abbrev};
        CREATE TABLE ind_{abbrev}.abs_density_net_{abbrev}  AS
        SELECT overall.{area_id},
               dwelling,
               area,
               dwelling/NULLIF(area,0) AS net_density,
               urban_dwelling,
               urban_area,
               urban_dwelling/NULLIF(urban_area,0) AS urban_net_density,
               not_urban_dwelling,
               not_urban_area,
               not_urban_dwelling /NULLIF(not_urban_area,0) AS not_urban_net_density
        FROM 
        (SELECT {area_id}, 
               COALESCE(SUM(dwelling),0) dwelling , 
               COALESCE(SUM(area_ha),0) area
        FROM area_linkage
        WHERE mb_category_name_2016 = 'Residential'
        GROUP BY {area_id}) AS overall
        LEFT JOIN 
        (SELECT {area_id}, 
                COALESCE(SUM(dwelling),0) urban_dwelling, 
                COALESCE(SUM(area_ha),0)  urban_area
        FROM area_linkage
        WHERE mb_category_name_2016 = 'Residential'
          AND urban = 'urban' 
        GROUP BY {area_id}) AS urban ON overall.{area_id} = urban.{area_id}
        LEFT JOIN 
        (SELECT {area_id}, 
                COALESCE(SUM(dwelling),0) not_urban_dwelling, 
                COALESCE(SUM(area_ha),0)  not_urban_area
        FROM area_linkage
        WHERE mb_category_name_2016 = 'Residential'
          AND urban = 'not urban' 
        GROUP BY {area_id}) AS not_urban ON overall.{area_id} = not_urban.{area_id}; 
        CREATE INDEX abs_density_net_{abbrev}_ix ON ind_{abbrev}.abs_density_net_{abbrev} ({area_id});
      '''.format(abbrev = abbrev,
                 area_id = area_id)
  else:  
      area_id = 'study_region'
      abbrev = 'region'
      include_region = 'study_region,'
      query = '''    
        -- Gross density indicator, using all Mesh Blocks as denominator
        DROP TABLE IF EXISTS ind_{abbrev}.abs_density_gross_{abbrev};
        CREATE TABLE ind_{abbrev}.abs_density_gross_{abbrev} AS
        SELECT overall.{area_id},
               dwelling,
               area,
               dwelling/NULLIF(area,0) AS gross_density,
               urban_dwelling,
               urban_area,
               urban_dwelling/NULLIF(urban_area,0) AS urban_gross_density,
               not_urban_dwelling,
               not_urban_area,
               not_urban_dwelling /NULLIF(not_urban_area,0) AS not_urban_gross_density
        FROM 
        (SELECT '{study_region}'::text AS {area_id}, 
                COALESCE(SUM(dwelling),0) dwelling , 
               COALESCE(SUM(area_ha),0) area
        FROM area_linkage
        WHERE area_linkage.{area_id} = 't'
        GROUP BY {area_id}) AS overall
        LEFT JOIN 
        (SELECT '{study_region}'::text AS {area_id}, 
                COALESCE(SUM(dwelling),0) urban_dwelling, 
                COALESCE(SUM(area_ha),0)  urban_area
        FROM area_linkage
        WHERE urban = 'urban' 
          AND area_linkage.{area_id} = 't'
        GROUP BY {area_id}) AS urban ON overall.{area_id} = urban.{area_id}
        LEFT JOIN 
        (SELECT '{study_region}'::text AS {area_id}, 
                COALESCE(SUM(dwelling),0) not_urban_dwelling, 
                COALESCE(SUM(area_ha),0)  not_urban_area
        FROM area_linkage
        WHERE urban = 'not urban' 
          AND area_linkage.{area_id} = 't'
        GROUP BY {area_id}) AS not_urban ON overall.{area_id} = not_urban.{area_id};
        CREATE INDEX abs_density_gross_{abbrev}_ix ON ind_{abbrev}.abs_density_gross_{abbrev} ({area_id});
        
        -- Net density indicator, using residential Mesh Blocks as denominator
        DROP TABLE IF EXISTS ind_{abbrev}.abs_density_net_{abbrev};
        CREATE TABLE ind_{abbrev}.abs_density_net_{abbrev}  AS
        SELECT overall.{area_id},
               dwelling,
               area,
               dwelling/NULLIF(area,0) AS net_density,
               urban_dwelling,
               urban_area,
               urban_dwelling/NULLIF(urban_area,0) AS urban_net_density,
               not_urban_dwelling,
               not_urban_area,
               not_urban_dwelling /NULLIF(not_urban_area,0) AS not_urban_net_density
        FROM 
        (SELECT '{study_region}'::text AS {area_id}, 
               COALESCE(SUM(dwelling),0) dwelling , 
               COALESCE(SUM(area_ha),0) area
        FROM area_linkage
        WHERE mb_category_name_2016 = 'Residential'
          AND area_linkage.{area_id} = 't'
        GROUP BY {area_id}) AS overall
        LEFT JOIN 
        (SELECT '{study_region}'::text AS {area_id}, 
                COALESCE(SUM(dwelling),0) urban_dwelling, 
                COALESCE(SUM(area_ha),0)  urban_area
        FROM area_linkage
        WHERE mb_category_name_2016 = 'Residential'
          AND urban = 'urban' 
          AND area_linkage.{area_id} = 't'
        GROUP BY {area_id}) AS urban ON overall.{area_id} = urban.{area_id}
        LEFT JOIN 
        (SELECT '{study_region}'::text AS {area_id}, 
                COALESCE(SUM(dwelling),0) not_urban_dwelling, 
                COALESCE(SUM(area_ha),0)  not_urban_area
        FROM area_linkage
        WHERE mb_category_name_2016 = 'Residential'
          AND urban = 'not urban'
          AND area_linkage.{area_id} = 't' 
        GROUP BY {area_id}) AS not_urban ON overall.{area_id} = not_urban.{area_id}; 
        CREATE INDEX abs_density_net_{abbrev}_ix ON ind_{abbrev}.abs_density_net_{abbrev} ({area_id});
      '''.format(abbrev = abbrev,
                 area_id = area_id,
                 study_region = full_locale)
  
  # print(query)
  curs.execute(query)
  conn.commit()
  print("Done.")


print("Create housing diversity indicators... ")
# We drop these tables first, since some destinations may have been processed since previously running.
# These queries are quick to run, so not much cost to drop and create again.
for area in ['SA1', 'SA2','Suburb', 'LGA']:
  area_id = df_regions.loc[area,'id']
  abbrev = df_regions.loc[area,'abbreviation']
  print("{}... ".format(area)),
  path = '../data/ABS/derived/housing_diversity/housing_diversity_gini_index/'
  file = '2020-01-16 - Australian housing diversity by {abbrev} 2016.csv'.format(abbrev = abbrev.upper())
  diversity = pandas.read_csv('{path}{file}'.format(file = file,path = path), index_col=0)
  diversity.to_sql('abs_housing_diversity_{abbrev}'.format(abbrev=abbrev),engine,if_exists='replace',schema='ind_{}'.format(abbrev))
  print("Done.")

print("Creating ABS IRSD 2011 and 2016 indicators at SA1 and SA2 levels")

# Compile a list of dataframes to support look ups across years
df = {}
# retrieve area linkage table 
area_linkage_fields = {'sa1':['sa1_maincode_2016','sa1_7digitcode_2016'], 'sa2':['sa2_name_2016']}
# read in ABS 2011 to 2016 correspondence tables for SA1 and SA2
irsd_tables = {'sa1':{'linkage':['sa1_maincode_2016','sa1_7digitcode_2016'],
                      'correspondence':{'data':'../data/ABS/downloads/cg_sa1_2011_sa1_2016.xls',
                                        'sheet_name':'Table 3','skiprows':[0,1,2,3,4,5,6],
                                        'skipfooter':3,
                                        'cols':[1,2,3],
                                        'fields':['sa1_7digitcode_2011','sa1_maincode_2016','sa1_7digitcode_2016'],
                                        'idx':'sa1_maincode_2016'},
                      'irsd':{'2011':{'data':'../data/ABS/downloads/irsd/2011/2033.0.55.001 sa1 indexes.xls',
                                      'sheet_name':'Table 2',
                                      'skiprows':[0,1,2,3,4,5],
                                      'skipfooter':3,
                                      'cols':[0,2,4,5,6],
                                      'idx':'sa1_7digitcode_2011'},
                              '2016':{'data':'../data/ABS/downloads/irsd/2016/2033055001 - sa1 indexes.xls' ,'sheet_name':'Table 2','skiprows':[0,1,2,3,4,5],'skipfooter':2,'cols':[0,3,5,6,7],'idx':'sa1_7digitcode_2016'}}},
               'sa2':{'linkage':['sa2_name_2016'],
                      'correspondence':{'data':'../data/ABS/downloads/cg_sa2_2011_sa2_2016.xls',
                                        'sheet_name':'Table 3',
                                        'skiprows':[0,1,2,3,4,5,6],
                                        'skipfooter':3,
                                        'cols':[1,3],
                                        'fields':['sa2_name_2011','sa2_name_2016'],
                                        'idx':'sa2_name_2016'},
                   'irsd':{'2011':{'data':'../data/ABS/downloads/irsd/2011/2033.0.55.001 SA2 Indexes.xls',
                                   'sheet_name':'Table 2',
                                   'skiprows':[0,1,2,3,4,5],
                                   'skipfooter':3,
                                   'cols':[1,3,5,6,7],
                                   'idx':'sa2_name_2011'},
                           '2016':{'data':'../data/ABS/downloads/irsd/2016/2033055001 - sa2 indexes.xls' ,
                                   'sheet_name':'Table 2',
                                   'skiprows':[0,1,2,3,4,5],
                                   'skipfooter':2,
                                   'cols':[1,3,5,6,7],
                                   'idx':'sa2_name_2016'}}}
}

fields = ['irsd_aust_score','irsd_aust_rank','irsd_aust_decile','irsd_aust_pctile']
for area in irsd_tables:
    sql = '''
    SELECT DISTINCT ON ({}) 
           {}
    FROM area_linkage
    '''.format(irsd_tables[area]['linkage'][0],','.join(irsd_tables[area]['linkage']))
    # print(sql)
    area_linkage = pandas.read_sql(sql,engine)
    print(area)
    xls = pandas.ExcelFile(irsd_tables[area]['correspondence']['data'])
    df[area] = pandas.read_excel(xls, 
                                 irsd_tables[area]['correspondence']['sheet_name'], 
                                 header=None,
                                 skiprows=irsd_tables[area]['correspondence']['skiprows'],
                                 skipfooter=irsd_tables[area]['correspondence']['skipfooter'])[irsd_tables[area]['correspondence']['cols']]   
    df[area].columns =   [irsd_tables[area]['correspondence']['fields']]
    df[area][irsd_tables[area]['correspondence']['idx']] = df[area][irsd_tables[area]['correspondence']['idx']].astype('str')
    df[area].set_index(irsd_tables[area]['correspondence']['idx'],inplace=True)
    df[area] = df[area].loc[area_linkage[df[area].index.name]].reset_index()
    for year in irsd_tables[area]['irsd']:
        irsd = 'abs_irsd_{year}_{area}'.format(year=year,area=area)
        xls = pandas.ExcelFile(irsd_tables[area]['irsd'][year]['data'])
        df[irsd] = pandas.read_excel(xls, 
                                     irsd_tables[area]['irsd'][year]['sheet_name'], 
                                     header=None,
                                     skiprows=irsd_tables[area]['irsd'][year]['skiprows'],
                                     skipfooter=irsd_tables[area]['irsd'][year]['skipfooter'])[irsd_tables[area]['irsd'][year]['cols']]   
        df[irsd].columns = [irsd_tables[area]['irsd'][year]['idx']]+['{}_{}_{}'.format(year,area,x) for x in fields]
        df[irsd].set_index(irsd_tables[area]['irsd'][year]['idx'],inplace=True)
        df[irsd] = df[irsd].loc[df[area][irsd_tables[area]['irsd'][year]['idx']]]
        df[irsd] = df[irsd].join(df[area].set_index(irsd_tables[area]['irsd'][year]['idx'])).dropna().drop_duplicates()
        df[irsd] = df[irsd].reset_index().set_index(irsd_tables[area]['linkage'][0])
        # make integer if possible
        for c in df[irsd].columns:
            try:
                df[irsd][c] = df[irsd][c].astype(int)
            except:
                continue
        df[irsd].to_sql(irsd,engine,schema='ind_{area}'.format(area=area),if_exists='replace')

# output to completion log
script_running_log(script, task, start)
conn.close()
