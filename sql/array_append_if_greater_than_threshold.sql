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