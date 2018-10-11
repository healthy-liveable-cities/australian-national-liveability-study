# Script:  who.py
# Purpose: who is processing which study regions?
# Author:  Carl Higgs
# Date:    20181009

# Import custom variables for National Liveability indicator process
from config_ntnl_li_process import pandas,responsible

print(responsible.reset_index().sort(['responsible','locale']).to_string(index=False))
