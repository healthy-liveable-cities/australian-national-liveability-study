title National Cities Performance Framework indicators for study regions
echo  National Cities Performance Framework indicators for study regions
echo  **NOTE: Assumes you have run AOS OD and OD to transport destinations scripts**

FOR %%A  IN (%*) DO (
  python 20_parcel_exclusion.py %%A
  python 21_NCPF_BITRE_summary.py %%A
)

@pause
