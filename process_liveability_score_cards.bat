title Process 2018/19 Liveability Score Cards (January 2020)
echo  Process 2018/19 Liveability Score Cards (January 2020)

FOR %%A  IN (%*) DO (
  python 18_neighbourhood_indicators.py %%A
  python 21_parcel_exclusion.py %%A
  python 22_composite_indicators.py %%A
  python 30_score_cards.py %%A
)

@pause



