title June 2020 open space indicator update
echo  June 2020 open space indicator update

FOR %%A  IN (%*) DO (
python 21_neighbourhood_indicators.py %%A 
python 23_parcel_exclusion.py %%A 
python 24_composite_indicators.py %%A 
python 25_collate_indicator_tables.py %%A 
python 26_data_checking.py %%A 
python 28_policy_indicators.py %%A 
python 31_score_cards.py %%A 
)

@pause







