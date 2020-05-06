import numpy as np
import pandas as pd

def clean_hhdata(rawdf):
    #- check for fipsstatecode and state
    fips_st=rawdf[['fipsstatecode','state']].drop_duplicates()
    

