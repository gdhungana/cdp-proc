import numpy as np
import pandas as pd

def get_cbsa_pop():
    """
    csvfile: -obtained from https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/metro/totals/cbsa-est2019-alldata.csv
    """
    cbsaall=pd.read_csv('./cbsa-est2019-alldata.csv',engine='python')
    cbsa_sel=cbsaall[pd.isna(cbsaall['STCOU'])]
    cbsa_sel=cbsa_sel[['CBSA','POPESTIMATE2018','POPESTIMATE2019']].sort_values('CBSA').reset_index(drop=True)
    return cbsa_sel

def get_cbsa_metmic(oldexcel):
    #-manually clean the first few rows and last few rows. remove rows with double . and remove the dots.
    # metro file from https://www2.census.gov/programs-surveys/popest/tables/2010-2019/metro/totals/cbsa-met-est2019-annres.xlsx
    cbsa_met=pd.read_excel('./cbsa-met-est2019-annres_1.xlsx',header=1)
    cbsa_mic=pd.read_excel('./cbsa-mic-est2019-annres_1.xlsx',header=1)
    cbsa_metmic=pd.concat([cbsa_met,cbsa_mic])
    cbsa_metmic=cbsa_metmic[['CBSA','2018','2019']].reset_index(drop=True)
    cbsa_metmic.rename(columns={2018:'Pop2018',2019:'Pop2019'},inplace=True)
    cbsa_metmic['CBSA']=cbsa_metmic['CBSA'].str.lstrip('.')
    return cbsa_metmic

