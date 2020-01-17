title Process late January 2020 preliminary results 
echo  Process late January 2020 preliminary results 

FOR %%A  IN (%*) DO (
  python 19_ABS_indicators.py           %%A
  python 23_collate_indicator_tables.py %%A
  python 24_data_checking.py            %%A
  python 27_area_indicators.py          %%A
  python 28_policy_indicators.py        %%A
)

@pause
