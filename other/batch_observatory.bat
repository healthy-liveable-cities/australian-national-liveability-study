title Recompile and output indicators for observatory
echo  Recompile and output indicators for observatory

python grant_query.py %*

FOR %%A  IN (%*) DO (
  python _clean_database.py %%A  
  python 19_neighbourhood_indicators.py %%A 
  python 21_parcel_indicators.py %%A 
  python 22_data_checking.py %%A 
  python 24_si_mix.py %%A 
  python 25_live_sa1_work_sa3.py %%A 
  python 26_uli.py %%A 
  python 27_area_indicators.py %%A   
  python _observatory_dump.py %%A
)

@pause
