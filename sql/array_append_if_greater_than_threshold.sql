-- return minimum value of an integer array (specifically here, used for distance to closest within 3200m)
CREATE OR REPLACE FUNCTION array_append_if_gr(distances int[],distance int,threshold int default 3200) returns int[] as $$
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
  IF ((distance <= threshold) OR (distance IS NULL)) 
      THEN RETURN distances;
  ELSE 
    RETURN array_append(distances,distance);
  END IF;
END;
$$
LANGUAGE plpgsql;  