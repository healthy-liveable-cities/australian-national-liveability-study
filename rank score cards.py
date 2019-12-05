
indicators = ['social_infrastructure_mix','closest_activity_centre','closest_alcohol_offlicence','frequent_pt_400m','large_pos_400m','pct_live_work_local_area','pct_30_40_affordable_housing']
for ind in indicators:
    print('\r\n-- {}'.format(ind))
    sql = '''
    SELECT * FROM 
    (SELECT row_number() over () AS rank,t.* 
      FROM (SELECT study_region, {ind} 
            FROM score_card_region_person 
            ORDER BY {ind} DESC) t) r 
    WHERE  rank IN (1,2,3,15,16,17);
    '''.format(ind = ind)
    print(sql)