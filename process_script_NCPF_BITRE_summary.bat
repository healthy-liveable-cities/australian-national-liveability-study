title National Cities Performance Framework indicators for study regions
echo  National Cities Performance Framework indicators for study regions
echo  **NOTE: Assumes you have run AOS OD and OD to transport destinations scripts**

FOR %%A  IN (%*) DO (
  python NCPF_BITRE_summary.py %%A
)

@pause
