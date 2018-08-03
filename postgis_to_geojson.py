year = 2016
for locale in ['adelaide','bris','canberra','darwin','hobart','melb','perth','syd']:
  for feature in ['boundaries_sa1','boundaries_ssc','boundaries_lga','li_map_sa1','li_map_ssc','li_map_lga','ind_description','urban_sos']:
    print('ogr2ogr -f GeoJSON {feature}_{locale}_{year}.json "PG:host=localhost dbname=observatory user=postgres password=***REMOVED***" {feature}_{locale}_{year}'.format(feature = feature, locale = locale, year = year))
  
  
