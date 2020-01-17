title NHSD redux: purge results, recompile, reanalyse distances for NHSD destinations
echo  NHSD redux: purge results, recompile, reanalyse distances for NHSD destinations
echo  **NOTE: Assumes you have the current version of destinations database, from 31 May 2019.**

FOR %%A  IN (%*) DO (
  python 19_ABS_indicators.py           %%A
  python 23_collate_indicator_tables.py %%A
  python 24_data_checking.py            %%A
  python 27_area_indicators.py          %%A
  python 28_policy_indicators.py        %%A
)

@pause
