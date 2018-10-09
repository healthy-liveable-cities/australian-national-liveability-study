
SELECT * 
FROM
(SELECT unnest(array['area: '        ||string_agg(DISTINCT(to_char(aos_ha,'FM999999999D99')),'') || ' Ha',
                    'amenity: '      ||string_agg(DISTINCT(obj->>'amenity'),', '),                  
                    'access: '       ||string_agg(DISTINCT(obj->>'access'),', '),  
                    'landuse: '      ||string_agg(DISTINCT(obj->>'landuse'),', '), 
                    'leisure: '      ||string_agg(DISTINCT(obj->>'leisure'),', '), 
                    'natural: '      ||string_agg(DISTINCT(obj->>'natural'),', '), 
                    'sport: '        ||string_agg(DISTINCT(obj->>'sport'),', '), 
                    'waterway: '     ||string_agg(DISTINCT(obj->>'waterway'),', '), 
                    'wood: '         ||string_agg(DISTINCT(obj->>'wood'),', '),
                    'in_school: '    ||string_agg(DISTINCT(obj->>'in_school'),', '),
                    'is_school: '    ||string_agg(DISTINCT(obj->>'is_school'),', '),
                    'water_feature: '||string_agg(DISTINCT(obj->>'water_feature'),', ')]) AS tags
FROM open_space_areas, 
     jsonb_array_elements(attributes) obj 
WHERE aos_id = 8059) t
WHERE tags IS NOT NULL;


SELECT attributes #>> '{leisure,natural}' as tags, count(*) as count 
FROM open_space_areas GROUP BY attributes #>> '{leisure,natural}';  


SELECT jsonb_pretty(jsonb_agg(attributes)) AS attributes
FROM open_space_areas 
WHERE aos_id = 8059
GROUP BY attributes #>> '{leisure,water_feature}'
;



SELECT aos_id, attributes
FROM   open_space_areas o, jsonb_array_elements(o.attributes) obj;
WHERE  obj->>'src' = 'foo.png';



with (SELECT obj
FROM open_space_areas, 
     jsonb_array_elements(attributes) obj 
WHERE aos_id = 8059) as t
SELECT * from jsonb_each(t.obj)


SELECT DISTINCT(jsonb_each(jsonb_array_elements(attributes))), COUNT(*) FROM open_space_areas  WHERE aos_id = 645 GROUP BY attributes;

SELECT DISTINCT(jsonb_each_text(jsonb_array_elements(attributes))), COUNT(*) FROM open_space_areas  WHERE aos_id = 645 GROUP BY attributes;


SELECT a->>'key' AS key,jsonb_pretty(jsonb_agg(DISTINCT(a->'value'))) AS value FROM (SELECT to_jsonb(jsonb_each(jsonb_array_elements(attributes - '{acceptable_linear_feature}' - '{medial_axis_length}' - '{area_ha}' - '{num_symdiff_convhull_geoms}' - '{os_id}' - '{roundness}'  ))) a FROM open_space_areas o WHERE aos_id = 8059 GROUP BY attributes ) t  GROUP BY key;



SELECT attributes - '{acceptable_linear_feature}' - '{medial_axis_length}' - '{area_ha}' - '{num_symdiff_convhull_geoms}' - '{os_id}' - '{roundness}' FROM open_space_areas o WHERE aos_id = 645;

SELECT a->>'key' AS key,jsonb_pretty(jsonb_agg(DISTINCT(a->'value'))) AS value FROM (SELECT to_jsonb(jsonb_each(jsonb_array_elements(attributes) - '{acceptable_linear_feature}' - '{medial_axis_length}' - '{area_ha}' - '{num_symdiff_convhull_geoms}' - '{os_id}' - '{roundness}'  )) a FROM open_space_areas o WHERE aos_id = 8059 GROUP BY attributes ) t  GROUP BY key;


SELECT numgeom, aos_ha, aos_ha_school FROM open_space_areas o,(SELECT ST_Transform(geom,7845) AS geom FROM sample_suburb_20 WHERE ssc_name_2016 = 'Amity') s WHERE o.geom && s.geom AND ST_Intersects(o.geom,s.geom);

SELECT a->>'key' AS key,string_agg(DISTINCT(a->>'value'),', ') AS distinct_values FROM (SELECT to_jsonb(jsonb_each(jsonb_array_elements(attributes) - 'acceptable_linear_feature' - 'medial_axis_length' - 'area_ha' - 'num_symdiff_convhull_geoms' - 'os_id' - 'roundness'  )) a FROM open_space_areas o,(SELECT ST_Transform(geom,7845) AS geom FROM sample_suburb_20 WHERE ssc_name_2016 = 'Amity') s WHERE o.geom && s.geom AND ST_Intersects(o.geom,s.geom) GROUP BY attributes) t  GROUP BY key;

SELECT jsonb_pretty(attributes) FROM open_space_areas o,(SELECT ST_Transform(geom,7845) AS geom FROM sample_suburb_20 WHERE ssc_name_2016 = 'Holmview') s WHERE ST_Intersects(o.geom,s.geom) 

SELECT a->>'key' AS key,string_agg(DISTINCT(a->>'value'),', ') AS distinct_values FROM (SELECT to_jsonb(jsonb_each(jsonb_array_elements(attributes) - 'acceptable_linear_feature' - 'medial_axis_length' - 'area_ha' - 'num_symdiff_convhull_geoms' - 'os_id' - 'roundness'  )) a FROM open_space_areas o,(SELECT ST_Transform(geom,7845) AS geom FROM sample_suburb_20 WHERE ssc_name_2016 = 'Holmview') s WHERE ST_Intersects(o.geom,s.geom) GROUP BY attributes) t  GROUP BY key;

WITH 
area AS (SELECT o.* 
         FROM open_space_areas o,
              (SELECT ST_Transform(geom,7845) AS geom FROM sample_suburb_20 WHERE ssc_name_2016 = 'Holmview') s 
         WHERE o.geom && s.geom AND ST_Intersects(o.geom,s.geom)),
raw_num AS (SELECT aos_id,
                   numgeom, 
                   (jsonb_array_elements(attributes)->>'area_ha')::numeric AS os_ha, 
                   aos_ha::numeric, 
                   aos_ha_school::numeric,
                   school_os_percent::numeric FROM area),
d AS (SELECT aos_id, 
             numgeom, 
             ROUND(AVG(os_ha),2) || ' (' || ROUND(MIN(os_ha),2) || ' - ' || ROUND(MAX(os_ha),2) || ')' AS mean_range_os_ha, 
             aos_ha AS aos_ha, 
             aos_ha_school,
             school_os_percent             
      FROM raw_num 
      GROUP BY aos_id, numgeom, aos_ha, aos_ha_school,school_os_percent),
aos_averages AS (SELECT ROUND(AVG(d.aos_ha),2)  || ' (' || ROUND(MIN(d.aos_ha),2)  || ' - ' || ROUND(MAX(d.aos_ha),2)  || ')' AS mean_range_aos_ha,
                        ROUND(AVG(d.aos_ha_school),2)  || ' (' || ROUND(MIN(d.aos_ha_school),2)  || ' - ' || ROUND(MAX(d.aos_ha_school),2)  || ')' AS mean_range_aos_ha_school,
                        ROUND(AVG(d.numgeom),2) || ' (' || ROUND(MIN(d.numgeom),2) || ' - ' || ROUND(MAX(d.numgeom),2) || ')' AS mean_range_numgeom,
                        ROUND(AVG(r.os_ha),2) || ' (' || ROUND(MIN(r.os_ha),2) || ' - ' || ROUND(MAX(r.os_ha),2) || ')' AS mean_range_os_ha,
                        COUNT(DISTINCT(d.aos_id)) AS aos_count,
                        COUNT(DISTINCT(d.aos_ha_school)) AS school_count,
                        ROUND(AVG(d.school_os_percent),2)  || ' (' || ROUND(MIN(d.school_os_percent),2)  || ' - ' || ROUND(MAX(d.school_os_percent),2)  || ')' AS mean_range_school_os_percent
            FROM d,raw_num r),
num AS (SELECT jsonb_agg(jsonb_strip_nulls(to_jsonb(aos_averages))) AS numeric_summary FROM d,aos_averages),
str AS (SELECT to_jsonb(jsonb_each(jsonb_array_elements(attributes || numeric_summary ) - 'acceptable_linear_feature' - 'medial_axis_length' - 'area_ha' - 'num_symdiff_convhull_geoms' - 'os_id' - 'roundness'  )) att FROM area,num)
SELECT att->>'key' AS key,string_agg(DISTINCT(att->>'value'),', ') AS distinct_values FROM str GROUP BY key;


WITH 
area AS (SELECT o.* 
         FROM open_space_areas o,
              (SELECT ST_Transform(geom,7845) AS geom FROM sample_suburb_20 WHERE ssc_name_2016 = 'Browns Plains (Qld)') s 
         WHERE o.geom_w_schools && s.geom AND ST_Intersects(o.geom_w_schools,s.geom)),
raw_num AS (SELECT aos_id,
                   numgeom, 
                   (jsonb_array_elements(attributes)->>'area_ha')::numeric AS os_ha, 
                   aos_ha::numeric, 
                   aos_ha_school::numeric,
                   school_os_percent::numeric FROM area),
d AS (SELECT aos_id, 
             numgeom, 
             ROUND(AVG(os_ha),2) || ' (' || ROUND(MIN(os_ha),2) || ' - ' || ROUND(MAX(os_ha),2) || ')' AS mean_range_os_ha, 
             aos_ha AS aos_ha, 
             aos_ha_school,
             school_os_percent             
      FROM raw_num 
      GROUP BY aos_id, numgeom, aos_ha, aos_ha_school,school_os_percent),
aos_averages AS (SELECT ROUND(AVG(d.aos_ha),2)  || ' (' || ROUND(MIN(d.aos_ha),2)  || ' - ' || ROUND(MAX(d.aos_ha),2)  || ')' AS mean_range_aos_ha,
                        ROUND(AVG(d.aos_ha_school),2)  || ' (' || ROUND(MIN(d.aos_ha_school),2)  || ' - ' || ROUND(MAX(d.aos_ha_school),2)  || ')' AS mean_range_aos_ha_school,
                        ROUND(AVG(d.numgeom),2) || ' (' || ROUND(MIN(d.numgeom),2) || ' - ' || ROUND(MAX(d.numgeom),2) || ')' AS mean_range_numgeom,
                        ROUND(AVG(r.os_ha),2) || ' (' || ROUND(MIN(r.os_ha),2) || ' - ' || ROUND(MAX(r.os_ha),2) || ')' AS mean_range_os_ha,
                        COUNT(DISTINCT(d.aos_id)) AS aos_count,
                        COUNT(DISTINCT(d.aos_ha_school)) AS school_count,
                        ROUND(AVG(d.school_os_percent),2)  || ' (' || ROUND(MIN(d.school_os_percent),2)  || ' - ' || ROUND(MAX(d.school_os_percent),2)  || ')' AS mean_range_school_os_percent
            FROM d,raw_num r),
num AS (SELECT jsonb_agg(jsonb_strip_nulls(to_jsonb(aos_averages))) AS numeric_summary FROM d,aos_averages),
str AS (SELECT to_jsonb(jsonb_each(jsonb_array_elements(attributes || numeric_summary ) - 'acceptable_linear_feature' - 'medial_axis_length' - 'area_ha' - 'num_symdiff_convhull_geoms' - 'os_id' - 'roundness'  )) att FROM area,num)
SELECT att->>'key' AS key,string_agg(DISTINCT(att->>'value'),', ') AS distinct_values FROM str GROUP BY key;

CREATE FUNCTION suburb_summary(text) RETURNS TABLE(key text, distinct_values text) AS 
    $$
    WITH 
    area AS (SELECT o.* 
             FROM open_space_areas o,
                  (SELECT ST_Transform(geom,7845) AS geom FROM sample_suburb_20 WHERE ssc_name_2016 = $1) s 
             WHERE o.geom_w_schools && s.geom AND ST_Intersects(o.geom_w_schools,s.geom)),
    raw_num AS (SELECT aos_id,
                       numgeom, 
                       (jsonb_array_elements(attributes)->>'area_ha')::numeric AS os_ha, 
                       aos_ha::numeric, 
                       aos_ha_school::numeric,
                       school_os_percent::numeric FROM area),
    d AS (SELECT aos_id, 
                 numgeom, 
                 ROUND(AVG(os_ha),2) || ' (' || ROUND(MIN(os_ha),2) || ' - ' || ROUND(MAX(os_ha),2) || ')' AS mean_range_os_ha, 
                 aos_ha AS aos_ha, 
                 aos_ha_school,
                 school_os_percent             
          FROM raw_num 
          GROUP BY aos_id, numgeom, aos_ha, aos_ha_school,school_os_percent),
    aos_averages AS (SELECT ROUND(AVG(d.aos_ha),2)  || ' (' || ROUND(MIN(d.aos_ha),2)  || ' - ' || ROUND(MAX(d.aos_ha),2)  || ')' AS mean_range_aos_ha,
                            ROUND(AVG(d.aos_ha_school),2)  || ' (' || ROUND(MIN(d.aos_ha_school),2)  || ' - ' || ROUND(MAX(d.aos_ha_school),2)  || ')' AS mean_range_aos_ha_school,
                            ROUND(AVG(d.numgeom),2) || ' (' || ROUND(MIN(d.numgeom),2) || ' - ' || ROUND(MAX(d.numgeom),2) || ')' AS mean_range_numgeom,
                            ROUND(AVG(r.os_ha),2) || ' (' || ROUND(MIN(r.os_ha),2) || ' - ' || ROUND(MAX(r.os_ha),2) || ')' AS mean_range_os_ha,
                            COUNT(DISTINCT(d.aos_id)) AS aos_count,
                            COUNT(DISTINCT(d.aos_ha_school)) AS school_count,
                            ROUND(AVG(d.school_os_percent),2)  || ' (' || ROUND(MIN(d.school_os_percent),2)  || ' - ' || ROUND(MAX(d.school_os_percent),2)  || ')' AS mean_range_school_os_percent
                FROM d,raw_num r),
    num AS (SELECT jsonb_agg(jsonb_strip_nulls(to_jsonb(aos_averages))) AS numeric_summary FROM d,aos_averages),
    str AS (SELECT to_jsonb(jsonb_each(jsonb_array_elements(attributes || numeric_summary ) - 'acceptable_linear_feature' - 'medial_axis_length' - 'area_ha' - 'num_symdiff_convhull_geoms' - 'os_id' - 'roundness'  )) att FROM area,num)
    SELECT att->>'key' AS key,string_agg(DISTINCT(att->>'value'),', ') AS distinct_values FROM str GROUP BY key;
    $$
    LANGUAGE SQL
    IMMUTABLE
    RETURNS NULL ON NULL INPUT;
    
SELECT * FROM suburb_summary('Amity');



SELECT aos_id,attributes
             FROM open_space_areas o,
                  (SELECT ST_Transform(geom,7845) AS geom FROM sample_suburb_20 WHERE ssc_name_2016 = 'Bryden') s 
             WHERE o.geom_w_schools && s.geom AND ST_Intersects(o.geom_w_schools,s.geom) GROUP BY aos_id;
             
             
             
# NOTE --- need to re-run excluding these and possibly more tags which can identify particular OS cases




SELECT tags - ARRAY['addr:city'         ,        
                    'addr:full'         ,        
                    'addr:place'        ,     
                    'addr:postcode'     ,        
                    'addr:province'     ,       
                    'addr:street'       ,        
                    'website'           ,        
                    'wikipedia'         ,        
                    'description'       ,
                    'old_name'          ,   
                    'name:aus'          ,
                    'name:en'           ,
                    'name:de'           ,
                    'name:fr'           ,
                    'name:es'           ,
                    'name:ru'           ,
                    'alt_name'          ,
                    'addr:housename'    ,      
                    'addr:housenumber'  ,      
                    'addr:interpolation',      
                    'name'              ,
                    'designation'       ,
                    'email'             ,
                    'phone'             ,
                    'ref:capad2014_osm' ,           
                    'nswlpi:cadid'      ,
                    'wikidata'          ,
                    'name:source:url'   ,
                    'url']
FROM open_space;

 
 osm_id                     
 access                     
 admin_level                
 aerialway                  
 aeroway                    
 amenity                    
 area                       
 barrier                    
 bicycle                    
 brand                      
 bridge                     
 boundary                   
 building                   
 construction               
 covered                    
 culvert                    
 cutting                    
 denomination               
 disused                    
 embankment                 
 foot                       
 generator:source           
 harbour                    
 highway                    
 historic                   
 horse                      
 intermittent               
 junction                   
 landuse                    
 layer                      
 leisure                    
 lock                       
 man_made                   
 military                   
 motorcar                   
 natural                    
 office                     
 oneway                     
 operator                   
 place                      
 population                 
 power                      
 power_source               
 public_transport           
 railway                    
 ref                        
 religion                   
 route                      
 service                    
 shop                       
 sport                      
 surface                    
 toll                       
 tourism                    
 tower:type                 
 tracktype                  
 tunnel                     
 water                      
 waterway                   
 wetland                    
 width                      
 wood                       
 z_order                    
 way_area                   
 tags                       
 way                        
 geom                       
 beach                      
 river                      
 os_id                      
 water_feature              
 area_ha                    
 medial_axis_length         
 amal_to_area_ratio         
 symdiff_convhull_geoms     
 num_symdiff_convhull_geoms 
 roundness                  
 linear_feature             
 acceptable_linear_feature  
 in_school                  
 is_school                  
 no_school_geom             