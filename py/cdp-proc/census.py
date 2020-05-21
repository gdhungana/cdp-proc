import pandas as pd
import numpy as np
import os, sys
import requests

def get_blk_commute(state,year,path):
    filename=state+'Commute'+year[2:]+'.csv'
    comm=pd.read_csv(datapath+'/'+filename)
    comm.drop(axis=0,index=0,inplace=True)
    comm=comm.reset_index()
    com_fields=[ii for ii in comm.columns.values if 'Commute' in ii and 'E' !=ii[-1] and 'N' not in ii[-1]]
    com_fields_i=[int(ii[7:]) for ii in com_fields]
    othfields=['BlkGrp','CommuteN']
    comm2=comm[othfields+com_fields].astype(int)
    comm2['AvgCommute']=sum(comm2[com_fields[ii]]*com_fields_i[ii] for ii in np.arange(len(com_fields)))/comm2['CommuteN']
    return comm2[['BlkGrp','CommuteN','AvgCommute']]

def get_blk_econ(state,year,path):
    filename=state+'Econ'+year[2:]+'.csv'
    econ=pd.read_csv(datapath+'/'+filename)
    econ.drop(axis=0,index=0,inplace=True)
    econ=econ.reset_index()
    #- fields
    LT50_fields=['LT10','LT15','LT20','LT25','LT30','LT35','LT40','LT45','LT50']
    GT100_fields=['GT100','GT125','GT150','GT200']
    GT125_fields=['GT125','GT150','GT200']
    GT150_fields=['GT150','GT200']
    #- convert the fields to float before calculation
    fulllist=list(set(LT50_fields+GT100_fields+GT125_fields+GT150_fields))
    print("Full list: ", fulllist)
    othfields=['BlkGrp','TotHse']
    econ=econ[fulllist+othfields].astype(int)
    #- Get the metrics
    econ['LT50p']=sum(econ[ii] for ii in LT50_fields)/econ['TotHse']
    econ['GT100p']=sum(econ[ii] for ii in GT100_fields)/econ['TotHse']
    econ['GT125p']=sum(econ[ii] for ii in GT125_fields)/econ['TotHse']
    econ['GT150p']=sum(econ[ii] for ii in GT150_fields)/econ['TotHse']
    econ['GT200p']=econ['GT200']/econ['TotHse']
    return econ[['BlkGrp','TotHse','LT50p','GT100p','GT125p','GT150p','GT200p']]

def get_blk_educ(state,year,path):
    filename=state+'Educ'+year[2:]+'.csv'
    educ=pd.read_csv(datapath+'/'+filename)
    educ.drop(axis=0,index=0,inplace=True)
    educ=educ.reset_index()
    educ_fields=['BACH','GRAD','PROF','PHD']
    othfields=['BlkGrp','POP25']
    educ=educ[educ_fields+othfields].astype(int)
    #- Get the stat.
    educ['BachPerc']=sum(educ[ii] for ii in educ_fields)/educ['POP25']
    return educ[['BlkGrp','POP25','BachPerc']]

def get_blk_latin(state,year,path):
    filename=state+'Latin'+year[2:]+'.csv'
    latin=pd.read_csv(datapath+'/'+filename)
    latin.drop(axis=0,index=0,inplace=True)
    latin=latin[['BlkGrp','TOTPOP','NotLat','Latin']]
    return latin

def get_blk_race(state,year,path):
    filename=state+'Race'+year[2:]+'.csv'
    race=pd.read_csv(datapath+'/'+filename)
    race.drop(axis=0,index=0,inplace=True)
    race=race[['BlkGrp','TOTPOP','WHIT','BLCK','AMIND','ASIA','HAWA','OthRce']]
    return race

def get_blk_poverty(state,year,path):
    filename=state+'Poverty'+year[2:]+'.csv'
    poverty=pd.read_csv(datapath+'/'+filename)
    poverty.drop(axis=0,index=0,inplace=True)
    pov_fields=['LTPov0toHalf','LTPovHalfto1']
    othfields=['BlkGrp','TotPop']
    poverty=poverty[pov_fields+othfields].astype(int)
    #- get the stat
    PovPerc=sum(poverty[ii] for ii in pov_fields)/poverty['TotPop'] 
    return poverty[['BlkGrp','TotPop','PovPerc']]

def get_blk_medhhinc(state,year,path):
    filename=state+'MedHHInc'+year[2:]+'.csv'
    medhh=pd.read_csv(datapath+'/'+filename)
    medhh.drop(axis=0,index=0,inplace=True)
    medhh=medhh[['BlkGrp','MedHInc']]
    return medhh

def get_state_map_census():
    url_St='https://api.census.gov/data/2018/acs/acs5/profile?get=NAME&for=state:*' 
    r=requests.get(url_St)
    files=r.json()
    df=pd.DataFrame(files[1:],columns=files[0])
    return df

get_track_data_acs(kind='econ',state='01',year='2018'):
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
    r=requests.get(url)
    files=r.json()
    print("Total lists ", len(files))
    df=pd.DataFrame(files[1:],columns=files[0])
    return df

