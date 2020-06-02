import numpy as np
import pandas as pd

def get_cbsa_pop(outfields=None):
    """
    csvfile: -obtained from https://www2.census.gov/programs-surveys/popest/datasets/2010-2019/metro/totals/cbsa-est2019-alldata.csv
    """
    if outfields is None:
        outfields=['CBSA','POPESTIMATE2017','POPESTIMATE2018','POPESTIMATE2019']
    cbsaall=pd.read_csv('./cbsa-est2019-alldata.csv',engine='python')
    cbsaall=cbsaall[outfields]
    cbsaall.drop_duplicates(inplace=True)
    cbsaall.reset_index(inplace=True,drop=True)
    #cbsamax['pop19']=cbsaall.groupby(['CBSA'])['POPESTIMATE2019'].max()
    #- index of max values
    idx = cbsaall.groupby(['CBSA'])['POPESTIMATE2019'].transform(max) == cbsaall['POPESTIMATE2019']
    cbsa_sel=cbsaall[idx].reset_index(drop=True)
    cbsa_sel['CBSA_n']=cbsa_sel['CBSA']
    cbsa_sel.drop(['CBSA'],axis=1,inplace=True)
    return cbsa_sel

def get_cbsa_metmic(outfields=None):
    if outfields is None:
        outfields=['CBSA',2018,2019]
    #-manually clean the first few rows and last few rows. remove rows with double . and remove the dots.
    # metro file from https://www2.census.gov/programs-surveys/popest/tables/2010-2019/metro/totals/cbsa-met-est2019-annres.xlsx
    cbsa_met=pd.read_excel('./cbsa-met-est2019-annres_1.xlsx',header=1)
    cbsa_mic=pd.read_excel('./cbsa-mic-est2019-annres_1.xlsx',header=1)
    cbsa_metmic=pd.concat([cbsa_met,cbsa_mic])
    cbsa_metmic=cbsa_metmic[outfields].reset_index(drop=True)
    cbsa_metmic.rename(columns={2018:'Pop2018',2019:'Pop2019'},inplace=True)
    cbsa_metmic['CBSA_n']=cbsa_metmic['CBSA'].str.lstrip('.')
    cbsa_metmic.drop(['CBSA'],axis=1,inplace=True)
    return cbsa_metmic

