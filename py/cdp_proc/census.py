import pandas as pd
import numpy as np
import os, sys
import json,re,requests
from requests.auth import HTTPBasicAuth

api_key='4d8a6dba79ca5499c89e527beb348364e4ef9ecd'
#auth = HTTPBasicAuth('apikey', api_key)
params = dict(key=api_key)

def get_blk_commute(commuteDF):
    #filename=state+'Commute'+year[2:]+'.csv'
    #comm=pd.read_csv(datapath+'/'+filename)
    #comm.drop(axis=0,index=0,inplace=True)
    #comm=comm.reset_index()
    #- check for none entries and remove them
    noneDF=commuteDF[pd.isna(commuteDF['CommuteN'])]
    if noneDF.shape[0]>0:
        print("Found None entries: ", noneDF.shape[0], "in states: ", set(noneDF['STATE'].values))
    comm=commuteDF[~pd.isna(commuteDF['CommuteN'])]
    com_fields=[ii for ii in comm.columns.values if 'Commute' in ii and 'E' !=ii[-1] and 'N' not in ii[-1]]
    com_fields_i=[int(ii[7:]) for ii in com_fields]
    othfields=['BlkGrp','CommuteN','YEAR']
    comm2=comm[othfields+com_fields].astype(int)
    comm2['AvgCommute']=sum(comm2[com_fields[ii]]*com_fields_i[ii] for ii in np.arange(len(com_fields)))/comm2['CommuteN']
    return comm2[['BlkGrp','YEAR','CommuteN','AvgCommute']]

def get_blk_econ(econDF):
    #filename=state+'Econ'+year[2:]+'.csv'
    #econ=pd.read_csv(datapath+'/'+filename)
    #econ.drop(axis=0,index=0,inplace=True)
    #econ=econ.reset_index()
    #- check for none entries and remove them
    noneDF=econDF[pd.isna(econDF['TotHse'])]
    if noneDF.shape[0]>0:
        print("Found None entries: ", noneDF.shape[0], "in states: ", set(noneDF['STATE'].values))
    econ=econDF[~pd.isna(econDF['TotHse'])]
    #- fields
    LT50_fields=['LT10','LT15','LT20','LT25','LT30','LT35','LT40','LT45','LT50']
    GT100_fields=['GT100','GT125','GT150','GT200']
    GT125_fields=['GT125','GT150','GT200']
    GT150_fields=['GT150','GT200']
    #- convert the fields to float before calculation
    fulllist=list(set(LT50_fields+GT100_fields+GT125_fields+GT150_fields))
    print("Full list: ", fulllist)
    othfields=['BlkGrp','TotHse','YEAR']
    econ=econ[fulllist+othfields].astype(int)
    #- Get the metrics
    econ['LT50p']=sum(econ[ii] for ii in LT50_fields)/econ['TotHse']
    econ['GT100p']=sum(econ[ii] for ii in GT100_fields)/econ['TotHse']
    econ['GT125p']=sum(econ[ii] for ii in GT125_fields)/econ['TotHse']
    econ['GT150p']=sum(econ[ii] for ii in GT150_fields)/econ['TotHse']
    econ['GT200p']=econ['GT200']/econ['TotHse']
    #econ['TotHse_econ']=econ['TotHse']
    return econ[['BlkGrp','YEAR','TotHse','LT50p','GT100p','GT125p','GT150p','GT200p']]

def get_blk_educ(educDF):
    #filename=state+'Educ'+year[2:]+'.csv'
    #educ=pd.read_csv(datapath+'/'+filename)
    #educ.drop(axis=0,index=0,inplace=True)
    #educ=educ.reset_index()
    #- check for none entries and remove them
    noneDF=educDF[pd.isna(educDF['POP25'])]
    if noneDF.shape[0]>0:
        print("Found None entries: ", noneDF.shape[0], "in states: ", set(noneDF['STATE'].values))
    educ=educDF[~pd.isna(educDF['POP25'])]
    educ_fields=['BACH','GRAD','PROF','PHD']
    grad_fields=['GRAD','PROF','PHD']
    othfields=['BlkGrp','POP25','YEAR']
    educ=educ[educ_fields+othfields].astype(int)
    #- Get the stat.
    educ['BachPlusp']=sum(educ[ii] for ii in educ_fields)/educ['POP25']
    educ['GradPlusp']=sum(educ[ii] for ii in grad_fields)/educ['POP25']
    #educ['POP25_educ']=educ['POP25']
    return educ[['BlkGrp','YEAR','POP25','BachPlusp','GradPlusp']]

def get_blk_latin(latinDF):
    #filename=state+'Latin'+year[2:]+'.csv'
    #latin=pd.read_csv(datapath+'/'+filename)
    #latin.drop(axis=0,index=0,inplace=True)
    #- check for none entries and remove them
    noneDF=latinDF[pd.isna(latinDF['TOTPOP'])]
    if noneDF.shape[0]>0:
        print("Found None entries: ", noneDF.shape[0], "in states: ", set(noneDF['STATE'].values))
    latin=latinDF[~pd.isna(latinDF['TOTPOP'])]
    #latin['TOTPOP_ethnicity']=latin[TOTPOP]
    latin=latin[['BlkGrp','YEAR','TOTPOP','NotLat','Latin']]
    return latin

def get_blk_race(raceDF):
    #filename=state+'Race'+year[2:]+'.csv'
    #race=pd.read_csv(datapath+'/'+filename)
    #race.drop(axis=0,index=0,inplace=True)
    #- check for none entries and remove them
    noneDF=raceDF[pd.isna(raceDF['TOTPOP'])]
    if noneDF.shape[0]>0:
        print("Found None entries: ", noneDF.shape[0], "in states: ", set(noneDF['STATE'].values))
    race=raceDF[~pd.isna(raceDF['TOTPOP'])]
    racefields=['BlkGrp','YEAR','TOTPOP','WHIT','BLCK','AMIND','ASIA','HAWA']
    race=race[racefields].astype(int)
    race['WHITP']=race['WHIT']/race['TOTPOP']
    race['BLCKP']=race['BLCK']/race['TOTPOP']
    race['AMINDP']=race['AMIND']/race['TOTPOP']
    race['ASIAP']=race['ASIA']/race['TOTPOP']
    race['HAWAP']=race['HAWA']/race['TOTPOP']
    #race['TOTPOP_race']=race['TOTPOP']
    race=race[['BlkGrp','YEAR','TOTPOP','WHITP','BLCKP','AMINDP','ASIAP','HAWAP']]
    return race

def get_blk_poverty(povertyDF):
    #filename=state+'Poverty'+year[2:]+'.csv'
    #poverty=pd.read_csv(datapath+'/'+filename)
    #poverty.drop(axis=0,index=0,inplace=True)
    #- check for none entries and remove them
    noneDF=povertyDF[pd.isna(povertyDF['TotPop'])]
    if noneDF.shape[0]>0:
        print("Found None entries: ", noneDF.shape[0], "in states: ", set(noneDF['STATE'].values))
    poverty=povertyDF[~pd.isna(povertyDF['TotPop'])]
    pov_fields=['LTPov0toHalf','LTPovHalfto1']
    othfields=['BlkGrp','TotPop','YEAR']
    poverty=poverty[pov_fields+othfields].astype(int)
    #- get the stat
    poverty['PovPerc']=sum(poverty[ii] for ii in pov_fields)/poverty['TotPop']
    #poverty['TOTPOP_poverty']=poverty['TotPop']
    return poverty[['BlkGrp','YEAR','TotPop','PovPerc']]

def get_blk_medhhinc(medhhincDF):
    #filename=state+'MedHHInc'+year[2:]+'.csv'
    #medhh=pd.read_csv(datapath+'/'+filename)
    #medhh.drop(axis=0,index=0,inplace=True)
    #- check for none entries and remove them
    noneDF=medhhincDF[pd.isna(medhhincDF['MedHInc'])]
    if noneDF.shape[0]>0:
        print("Found None entries: ", noneDF.shape[0], "in states: ", set(noneDF['STATE'].values))
    medhh=medhhincDF[~pd.isna(medhhincDF['MedHInc'])]
    medhh=medhh[['BlkGrp','YEAR','MedHInc']]
    return medhh

def load_acskey_fields(datapath,censustype='tract',kind='econ'):
    if censustype=='tract':
        datafile=datapath+'/'+'TractDataDefinition.xlsx'
        sheet='Tract'+kind
    elif censustype=='blkgrp':
        datafile=datapath+'/'+'BlockDataDefinition.xlsx'
        sheet='BlkGrp'+kind
    else:
        print("censustype can be only tract or blkgrp")
    keyDF=pd.read_excel(datafile,sheet_name=sheet)
    return keyDF

def get_state_map_census():
    url_St='https://api.census.gov/data/2018/acs/acs5/profile?get=NAME&for=state:*' 
    r=requests.get(url_St,params=params)
    files=r.json()
    #r_parsed = re.sub(r'^jsonp\d+\(|\)\s+$', '', r.text)
    #print(r_parsed)
    #files=json.loads(r_parsed)
    df=pd.DataFrame(files[1:],columns=files[0])
    return df

def get_state_county_map_census(state=None):
    if state is not None:
        url_cty='https://api.census.gov/data/2018/acs/acs5?get=NAME&for=county:*&in=state:'+state
    else:
        print("Getting counties for all state")
        url_cty='https://api.census.gov/data/2018/acs/acs5?get=NAME&for=county:*'
    r=requests.get(url_cty,params=params)
    files=r.json()
    df=pd.DataFrame(files[1:],columns=files[0])
    return df

def get_tract_data_acs(kind='econ',state='01',year='2018'):
    if kind not in ['demo','econ','educ','hshld']:
        print("kind: ", kind, " not supported. Put a valid type")
        sys.exit(0)
    else:
        print("Getting the tract level data from acs for ", kind, "for state:", state)
    if kind=='demo':
        url='https://api.census.gov/data/'+year+'/acs/acs5/profile?get=group(DP05)&for=tract:*&in=state:'+state+'&in=county:*'
    elif kind=='econ':
        url='https://api.census.gov/data/'+year+'/acs/acs5/profile?get=group(DP03)&for=tract:*&in=state:'+state+'&in=county:*'
    elif kind=='educ':
        url='https://api.census.gov/data/'+year+'/acs/acs5/subject?get=group(S1501)&for=tract:*&in=state:'+state+'&in=county:*'
    elif kind=='hshld':
        url='https://api.census.gov/data/'+year+'/acs/acs5/subject?get=group(S1101)&for=tract:*&in=state:'+state+'&in=county:*'
    r=requests.get(url,params=params)
    files=r.json()
    print("Total lists ", len(files))
    df=pd.DataFrame(files[1:],columns=files[0])
    return df

def get_blkgrp_data_acs(kind='econ',state='01',county='001',year='2018'):
    if kind not in ['econ','educ','medhhinc','latin','poverty','commute','race']:
        print("kind: ", kind, " not supported. Put a valid type")
        sys.exit(0)
    else:
        print("Getting the block group level data from acs for ", kind, "for state:", state, ", county: ", county)
    if kind=='econ':
        url='https://api.census.gov/data/'+year+'/acs/acs5?get=group(B19001)&for=block%20group:*&in=state:'+state+'&in=county:'+county
    elif kind=='medhhinc':
        url='https://api.census.gov/data/'+year+'/acs/acs5?get=group(B19013)&for=block%20group:*&in=state:'+state+'&in=county:'+county
    elif kind=='educ':
        url='https://api.census.gov/data/'+year+'/acs/acs5?get=group(B15003)&for=block%20group:*&in=state:'+state+'&in=county:'+county
    elif kind=='latin':
        url='https://api.census.gov/data/'+year+'/acs/acs5?get=group(B03003)&for=block%20group:*&in=state:'+state+'&in=county:'+county
    elif kind=='poverty':
        url='https://api.census.gov/data/'+year+'/acs/acs5?get=group(C17002)&for=block%20group:*&in=state:'+state+'&in=county:'+county
    elif kind=='commute':
        url='https://api.census.gov/data/'+year+'/acs/acs5?get=group(B08303)&for=block%20group:*&in=state:'+state+'&in=county:'+county
    elif kind=='race':
        url='https://api.census.gov/data/'+year+'/acs/acs5?get=group(B02001)&for=block%20group:*&in=state:'+state+'&in=county:'+county
    r=requests.get(url,params=params)
    files=r.json()
    print("Total lists ", len(files))
    df=pd.DataFrame(files[1:],columns=files[0])
    return df
