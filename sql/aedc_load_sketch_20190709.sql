-- Create database
CREATE DATABASE little_forest;

-- Connect to database
\c little_forest;

-- Define table which we will import data into (SQL cares about data types)
DROP TABLE IF EXISTS saplings;
CREATE TABLE saplings
(
project_id                                              text             PRIMARY KEY,
latitude                                                double precision ,
longitude                                               double precision ,
epsg                                                    integer          ,
match_distance_m                                        double precision ,
study_region                                            text             ,
locale                                                  text             ,
urban                                                   text             ,
sa1_maincode_2016                                       bigint           ,
sa2_name_2016                                           text             ,
sa3_name_2016                                           text             ,
sa4_name_2016                                           text             ,
ssc_name_2016                                           text             ,
lga_name_2016                                           text             ,
gccsa_name_2016                                         text             ,
state_name_2016                                         text             ,
sos_name_2016                                           text             ,
wa_dns_1600m_dd_2018                                    double precision ,
wa_dns_1600m_sc_2018                                    double precision ,
wa_sco_800m_dl_2018                                     double precision ,
wa_sco_1600m_dl_2018                                    double precision ,
wa_sco_800m_ll_2018                                     double precision ,
wa_sco_1600m_ll_2018                                    double precision ,
wa_sco_1600m_wa_2018                                    double precision ,
tr_darray_3200m_cl_any_2018                             integer[]        ,
tr_darray_3200m_cl_freq_2018                            integer[]        ,
tr_darray_3200m_cl_bus_2018                             integer[]        ,
tr_darray_3200m_cl_ferry_2018                           integer[]        ,
tr_darray_3200m_cl_train_2018                           integer[]        ,
tr_darray_3200m_cl_tram_2018                            integer[]        ,
co_darray_3200m_cl_activ_2018                           integer[]        ,
co_darray_3200m_cl_lib_2018                             integer[]        ,
co_darray_3200m_cl_community_centre_2018                integer[]        ,
ey_darray_3200m_cl_playg_2018                           integer[]        ,
ey_darray_3200m_cl_childcare_any_2018                   integer[]        ,
ey_darray_3200m_cl_childcare_ohsc_2018                  integer[]        ,
ey_darray_3200m_cl_childcare_presch_2018                integer[]        ,
ey_darray_3200m_cl_childcare_any_meet_nqs_2018          integer[]        ,
ey_darray_3200m_cl_childcare_any_exc_nqs_2018           integer[]        ,
ey_darray_3200m_cl_childcare_ohsc_meet_nqs_2018         integer[]        ,
ey_darray_3200m_cl_childcare_ohsc_exc_nqs_2018          integer[]        ,
ey_darray_3200m_cl_childcare_presch_meet_nqs_2018       integer[]        ,
ey_darray_3200m_cl_childcare_presch_exc_nqs_2018        integer[]        ,
ed_darray_3200m_cl_sch_primary_2018                     integer[]        ,
ed_score_closest_sch_naplan_primary_2018                double precision ,
ed_avg_800m_sch_naplan_primary_2018                     double precision ,
ed_avg_1600m_sch_naplan_primary_2018                    double precision ,
os_darray_3200m_pos_2018                                integer[]        ,
os_darray_3200m_pos_less_4k_sqm_2018                    integer[]        ,
os_darray_3200m_pos_greater_4k_sqm_2018                 integer[]        ,
os_darray_3200m_pos_greater_15k_sqm_2018                integer[]        ,
os_darray_3200m_pos_4k_10k_sqm_2018                     integer[]        ,
os_darray_3200m_pos_10k_50k_sqm_2018                    integer[]        ,
os_darray_3200m_pos_50k_200k_sqm_2018                   integer[]        ,
os_darray_3200m_pos_greater_200k_sqm_2018               integer[]        ,
os_dist_closest_pos_toilet_distance_3200m_2018          integer          ,
os_darray_3200m_cl_swimming_pool_osm_2018               integer[]        ,
os_darray_3200m_sport_2018                              integer[]        ,
fo_pct_3200m_healthfood_2018                            double precision ,
he_darray_3200m_cl_gp_nhsd_2017_2018                    integer[]        ,
he_dist_closest_gp_nhsd_2017_2018                       integer          ,
he_darray_3200m_cl_mch_2018                             integer[]        ,
he_dist_closest_mch_2018                                integer          ,
he_dist_closest_hospital_2018                           integer          ,
he_dist_closest_famsupport_2018                         integer          ,
he_dist_closest_childplay_2018                          integer          ,
he_dist_closest_earlyparent_2018                        integer          ,
he_dist_closest_integrated_2018                         integer          ,
he_dist_closest_pharmacy_nhsd_2017_2018                 integer          ,
he_dist_closest_dentist_nhsd_2017_2018                  integer          ,
he_dist_closest_immunis_2018                            integer          ,
he_dist_closest_famcounsel_2018                         integer          ,
he_dist_closest_gencounsel_2018                         integer          ,
he_dist_closest_ecintervention_2018                     integer          ,
he_dist_closest_mh_child_adolescent_2018                integer          ,
he_dist_closest_med_paed_2018                           integer          ,
he_dist_closest_mh_gen_2018                             integer          ,
he_dist_closest_mh_adult_2018                           integer          ,
he_dist_closest_psych_2018                              integer          ,
al_darray_3200m_cl_alcohol_onlicence_2018               integer[]        ,
al_darray_3200m_cl_alcohol_offlicence_hlc_2017_19_2018  integer[]        ,
aos_jsonb                                               jsonb
);

-- Import data
COPY saplings FROM 'D:/ntnl_li_2018_template/data/study_region/aedc/little_forest_final_matched_20190626.csv' WITH DELIMITER ',' CSV HEADER;


-- Define indices on data
CREATE INDEX ginx_aos_jsb ON saplings USING gin (aos_jsonb);
CREATE INDEX ix_od_aos_jsonb_aos_id ON saplings USING btree (((aos_jsonb -> 'aos_id'::text)));
CREATE INDEX ix_od_aos_jsonb_distance ON saplings USING btree (((aos_jsonb -> 'distance'::text)));

-- Define utility functions

-- Function for returning counts of values in an array less than a threshold distance
-- e.g. an array of distances in m to destinations, evaluated against a threshold of 800m
-- SELECT gnaf_pid, count_in_threshold(distances,1600) FROM sport_3200m;
-- is equivalent to 
-- SELECT gnaf_pid, count(distances) 
--   FROM (SELECT gnaf_pid,unnest(array_agg) distances FROM sport_3200m) t 
-- WHERE distance < 1600 GROUP BY gnaf_pid;
CREATE OR REPLACE FUNCTION count_in_threshold(distances int[],threshold int) returns bigint as $$
    SELECT COUNT(*) 
    FROM unnest(distances) dt(b)
    WHERE b < threshold
$$ language sql;

-- return minimum value of an integer array (specifically here, used for distance to closest within 3200m)
CREATE OR REPLACE FUNCTION array_min(integers int[]) returns int as $$
    SELECT min(integers) 
    FROM unnest(integers) integers
$$ language sql;

-- append value to array if > some threshold (default, 3200)
CREATE OR REPLACE FUNCTION array_append_if_gr(distances int[],distance int,threshold int default 3200) returns int[] as $$
BEGIN
-- function to append an integer to an array of integers if it is larger than some given threshold 
-- (ie. add in distance to closest to 3200m distances array if the distance to closest value is > 3200m
-- Example applied usage:
-- SELECT gnaf_pid, 
        -- array_append_if_gr(dests.alcohol_offlicence,cl.alcohol_offlicence) AS array,
        -- cl.alcohol_offlicence AS distance
-- FROM dest_distances_3200m dests 
-- LEFT JOIN dest_distance_m cl
-- USING (gnaf_pid) 
-- WHERE cl.alcohol_offlicence > 3200;
IF ((distance <= threshold) OR (distance IS NULL)) 
    THEN RETURN distances;
ELSE 
    RETURN array_append(distances,distance);
END IF;
END;
$$
LANGUAGE plpgsql;  

-- a binary threshold indicator  (e.g. of access given distance and threshold)
CREATE OR REPLACE FUNCTION threshold_hard(distance int, threshold int, out int) 
    RETURNS NULL ON NULL INPUT
    AS $$ SELECT (distance < threshold)::int $$
    LANGUAGE SQL;

-- a soft threshold indicator (e.g. of access given distance and threshold)
CREATE OR REPLACE FUNCTION threshold_soft(distance int, threshold int) returns float AS 
$$
BEGIN
  -- We check to see if the value we are exponentiation is more or less than 100; if so,
  -- if so the result will be more or less either 1 or 0, respectively. 
  -- If the value we are exponentiating is much > abs(700) then we risk overflow/underflow error
  -- due to the value exceeding the numerical limits of postgresql
  -- If the value we are exponentiating is based on a positive distance, then we know it is invalid!
  -- For reference, a 10km distance with 400m threshold yields a check value of -120, 
  -- the exponent of which is 1.30418087839363e+052 and 1 - 1/(1+exp(-120)) is basically 1 - 1 = 0
  -- Using a check value of -100, the point at which zero is returned with a threshold of 400 
  -- is for distance of 3339km
  IF (distance < 0) 
      THEN RETURN NULL;
  ELSIF (-5*(distance-threshold)/(threshold::float) < -100) 
    THEN RETURN 0;
  ELSE 
    RETURN 1 - 1/(1+exp(-5*(distance-threshold)/(threshold::float)));
  END IF;
END;
$$
LANGUAGE plpgsql
RETURNS NULL ON NULL INPUT;  



-- Add a new column for a 'distance to closest POS indicator', with an 'integer' datatype (int for short)
ALTER TABLE saplings ADD COLUMN os_cl_any_pos_dist_m integer;

-- Set new distance to POS indicator to equal the smallest value in the distances to all POS in 3200m list
UPDATE saplings SET os_cl_any_pos_dist_m = array_min(os_darray_3200m_pos_2018);

-- Have a quick preview of the new indicator you've created and calculated (limited to 100 observations):
SELECT project_id, os_darray_3200m_pos_2018, os_cl_any_pos_dist_m FROM saplings LIMIT 100;


















































































































































