title NHSD redux: purge results, recompile, reanalyse distances for NHSD destinations
echo  NHSD redux: purge results, recompile, reanalyse distances for NHSD destinations
echo  **NOTE: Assumes you have the current version of destinations database, from 31 May 2019.**

FOR %%A  IN (%*) DO (
  python purge_old_nhsd_20190531.py %%A
  python 14_recompile_destinations.py %%A
)

@pause



