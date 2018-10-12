# Script:  who.py
# Purpose: who is processing which study regions?
# Author:  Carl Higgs
# Date:    20181009

# Import custom variables for National Liveability indicator process
# import modules
import os
import sys
import pandas

# Load settings from ind_study_region_matrix.xlsx
xls = pandas.ExcelFile(os.path.join(sys.path[0],'ind_study_region_matrix.xlsx'))
df_about = pandas.read_excel(xls, 'about')
df_studyregion = pandas.read_excel(xls, 'study_regions',index_col=1)

responsible = df_studyregion['responsible']
print(df_about.iloc[0].reset_index()['index'].to_string(index=False).encode('utf'))
print(responsible.reset_index().sort_values(by=['responsible','locale']).to_string(index=False))

