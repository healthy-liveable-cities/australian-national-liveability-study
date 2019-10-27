CREATE TABLE IF NOT EXISTS dwellings 
(
 mb_code_2016          text PRIMARY KEY,
 mb_category_name_2016 text             ,
 area_albers_sqkm      double precision ,
 dwelling              double precision ,
 person                bigint           ,
 state                 bigint           
 );
 
COPY dwellings FROM 'D:/ABS/data/2016/derived/2016_census_mesh_block_counts.csv' DELIMITER ',' CSV HEADER;
 
CREATE TABLE IF NOT EXISTS mb_ssc_aust_2016
(
 mb_code_2016 text PRIMARY KEY,
 ssc_code_2016 text,
 ssc_name_2016 text,
 state_code_2016 text,
 state_name_2016 text,
 area_albers_sqkm double precision
);
COPY mb_ssc_aust_2016 FROM 'D:/ABS/data/2016/SSC_2016_AUST.csv' DELIMITER ',' CSV HEADER;
 
 
CREATE TABLE IF NOT EXISTS mb_lga_aust_2016
(
 mb_code_2016 text PRIMARY KEY,
 lga_code_2016 text,
 lga_name_2016 text,
 state_code_2016 text,
 state_name_2016 text,
 area_albers_sqkm double precision
);
COPY mb_lga_aust_2016 FROM 'D:/ABS/data/2016/LGA_2016_ACT.csv' DELIMITER ',' CSV HEADER;
COPY mb_lga_aust_2016 FROM 'D:/ABS/data/2016/LGA_2016_NSW.csv' DELIMITER ',' CSV HEADER;
COPY mb_lga_aust_2016 FROM 'D:/ABS/data/2016/LGA_2016_NT.csv'  DELIMITER ',' CSV HEADER;
COPY mb_lga_aust_2016 FROM 'D:/ABS/data/2016/LGA_2016_OT.csv'  DELIMITER ',' CSV HEADER;
COPY mb_lga_aust_2016 FROM 'D:/ABS/data/2016/LGA_2016_QLD.csv' DELIMITER ',' CSV HEADER;
COPY mb_lga_aust_2016 FROM 'D:/ABS/data/2016/LGA_2016_SA.csv'  DELIMITER ',' CSV HEADER;
COPY mb_lga_aust_2016 FROM 'D:/ABS/data/2016/LGA_2016_TAS.csv' DELIMITER ',' CSV HEADER;
COPY mb_lga_aust_2016 FROM 'D:/ABS/data/2016/LGA_2016_VIC.csv' DELIMITER ',' CSV HEADER;
COPY mb_lga_aust_2016 FROM 'D:/ABS/data/2016/LGA_2016_WA.csv'  DELIMITER ',' CSV HEADER;

CREATE TABLE IF NOT EXISTS abs_2016_irsd 
(
sa1_maincode_2016    text PRIMARY KEY,
sa1_7digit           text,
usual_resident_pop   integer,
irsd_score           integer,
aust_rank            integer,
aust_decile          integer,
aust_pctile          integer,
state                text,
state_rank           integer,
state_decile         integer,
state_pctile         integer
);
COPY abs_2016_irsd FROM 'D:/ABS/data/2016/derived/ABS_2016_IRSD.csv'  DELIMITER ',' CSV HEADER;

CREATE TABLE ntnl_mb_stats AS
SELECT
a.mb_code_2016          ,
a.mb_category_name_2016 ,
a.sa1_maincode_2016     ,
a.sa1_7digitcode_2016   ,
a.sa2_maincode_2016     ,
a.sa2_5digitcode_2016   ,
a.sa2_name_2016         ,
a.sa3_code_2016         ,
a.sa3_name_2016         ,
a.sa4_code_2016         ,
a.sa4_name_2016         ,
a.gccsa_code_2016       ,
a.gccsa_name_2016       ,
s.ssc_code_2016         ,
s.ssc_name_2016         ,
l.lga_code_2016         ,
l.lga_name_2016         ,
a.state_code_2016       ,
a.state_name_2016       ,
a.area_albers_sqkm      ,
d.dwelling              ,
d.person                ,
a.geom                  
FROM mb_2016_aust a
LEFT JOIN mb_ssc_aust_2016 s ON a.mb_code_2016 = s.mb_code_2016
LEFT JOIN mb_lga_aust_2016 l ON a.mb_code_2016 = l.mb_code_2016
LEFT JOIN dwellings d        ON a.mb_code_2016 = d.mb_code_2016
;
CREATE INDEX IF NOT EXISTS ntnl_mb_stats_idx ON ntnl_mb_stats (mb_code_2016);

CREATE TABLE test AS
SELECT 
      a.lga_code_2016                                  ,
      a.lga_name_2016                                  ,
      res_dwelling_total                               ,
      SUM(a.dwelling) AS dwelling_total                ,
      res_person_total                                 ,
      SUM(a.person) AS person_total                    ,
      SUM(area_albers_sqkm)*100 AS res_mb_area_total_ha,
      SUM(a.area_albers_sqkm)*100 AS lga_area_total_ha
      FROM ntnl_mb_stats a
      LEFT JOIN (SELECT lga_code_2016,
                 SUM(dwelling) AS res_dwelling_total,
                 SUM(person)   AS res_person_total,
                 SUM(area_albers_sqkm)*100 AS res_mb_area_total_ha
          FROM ntnl_mb_stats
          WHERE mb_category_name_2016 = 'Residential'
          GROUP BY lga_code_2016) n ON a.lga_code_2016 = n.lga_code_2016
     GROUP BY a.lga_code_2016,a.lga_name_2016, res_dwelling_total, res_person_total;


CREATE TABLE IF NOT EXISTS lga_densities_2016 AS
SELECT 
lga_code_2016                                                           ,
lga_name_2016                                                           ,
res_dwelling_total                                                      ,
dwelling_total                                                          ,
res_person_total                                                        ,
person_total                                                            ,
res_mb_area_total_ha                                                    ,
lga_area_total_ha                                                       ,
res_dwelling_total / res_mb_area_total_ha::float AS net_dwellings_per_ha,
dwelling_total / lga_area_total_ha:: float AS gross_dwellings_per_ha
FROM (SELECT 
      a.lga_code_2016                                  ,
      a.lga_name_2016                                  ,
      res_dwelling_total                               ,
      SUM(a.dwelling) AS dwelling_total                ,
      res_person_total                                 ,
      SUM(a.person) AS person_total                    ,
      res_mb_area_total_ha,
      SUM(a.area_albers_sqkm)*100 AS lga_area_total_ha
      FROM ntnl_mb_stats a
      LEFT JOIN (SELECT lga_code_2016,
                 SUM(dwelling) AS res_dwelling_total,
                 SUM(person)   AS res_person_total,
                 SUM(area_albers_sqkm)*100 AS res_mb_area_total_ha
          FROM ntnl_mb_stats
          WHERE mb_category_name_2016 = 'Residential'
          GROUP BY lga_code_2016) n USING(lga_code_2016)
     GROUP BY a.lga_code_2016,a.lga_name_2016, res_dwelling_total, res_person_total,res_mb_area_total_ha
     ) t
ORDER BY lga_code_2016;

CREATE TABLE IF NOT EXISTS ssc_densities_2016 AS
SELECT 
ssc_code_2016                                                           ,
ssc_name_2016                                                           ,
res_dwelling_total                                                      ,
dwelling_total                                                          ,
res_person_total                                                        ,
person_total                                                            ,
res_mb_area_total_ha                                                    ,
ssc_area_total_ha                                                       ,
res_dwelling_total / res_mb_area_total_ha::float AS net_dwellings_per_ha,
dwelling_total / ssc_area_total_ha:: float AS gross_dwellings_per_ha
FROM (SELECT 
      a.ssc_code_2016                                  ,
      a.ssc_name_2016                                  ,
      res_dwelling_total                               ,
      SUM(a.dwelling) AS dwelling_total                ,
      res_person_total                                 ,
      SUM(a.person) AS person_total                    ,
      res_mb_area_total_ha,
      SUM(a.area_albers_sqkm)*100 AS ssc_area_total_ha
      FROM ntnl_mb_stats a
      LEFT JOIN (SELECT ssc_code_2016,
                 SUM(dwelling) AS res_dwelling_total,
                 SUM(person)   AS res_person_total,
                 SUM(area_albers_sqkm)*100 AS res_mb_area_total_ha
          FROM ntnl_mb_stats
          WHERE mb_category_name_2016 = 'Residential'
          GROUP BY ssc_code_2016) n USING(ssc_code_2016)
     GROUP BY a.ssc_code_2016,a.ssc_name_2016, res_dwelling_total, res_person_total,res_mb_area_total_ha
     ) t
ORDER BY ssc_code_2016;       

CREATE TABLE IF NOT EXISTS sa1_densities_2016 AS
SELECT 
sa1_maincode_2016                                                       ,
res_dwelling_total                                                      ,
dwelling_total                                                          ,
res_person_total                                                        ,
person_total                                                            ,
res_mb_area_total_ha                                                    ,
sa1_area_total_ha                                                       ,
res_dwelling_total / res_mb_area_total_ha::float AS net_dwellings_per_ha,
dwelling_total / sa1_area_total_ha:: float AS gross_dwellings_per_ha
FROM (SELECT 
      a.sa1_maincode_2016                              ,
      res_dwelling_total                               ,
      SUM(a.dwelling) AS dwelling_total                ,
      res_person_total                                 ,
      SUM(a.person) AS person_total                    ,
      res_mb_area_total_ha                             ,
      SUM(a.area_albers_sqkm)*100 AS sa1_area_total_ha
      FROM ntnl_mb_stats a
      LEFT JOIN (SELECT sa1_maincode_2016,
                 SUM(dwelling) AS res_dwelling_total,
                 SUM(person)   AS res_person_total,
                 SUM(area_albers_sqkm)*100 AS res_mb_area_total_ha
          FROM ntnl_mb_stats
          WHERE mb_category_name_2016 = 'Residential'
          GROUP BY sa1_maincode_2016) n USING(sa1_maincode_2016)
     GROUP BY a.sa1_maincode_2016, res_dwelling_total, res_person_total,res_mb_area_total_ha
     ) t
ORDER BY sa1_maincode_2016;       


