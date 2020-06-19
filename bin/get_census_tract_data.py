#!/usr/bin/env python
import numpy as np
import pandas as pd
import os,sys
from cdp_proc import census as cn
import argparse
parser=argparse.ArgumentParser()
parser.add_argument("--censuslevel",required=True,help='the census level: tract or blkgrp')
parser.add_argument("--mapdatapath",required=False,default='../data',help='map to data path, need for API')
parser.add_argument("--kind", required=True,type=str, help='kind of information for the given census level')
parser.add_argument("--year", required=False,type=str, help='year to process data for, need for API')
parser.add_argument("--state",default=None,type=str,help='state to run the block group export on')
parser.add_argument("--outcsv", required=False, help='output csv if given')
parser.add_argument("--outsql", action='store_true', help='switch to write to a sql, db table name hardcoded')
parser.add_argument("--static_datapath", default='None',required=False, help='use static data files instead of API calls')
args = parser.parse_args()

def get_census_tract_data(kind,year,mapdatapath):
    #- get statemap first
    print("Getting the state map")
    stateDF=cn.get_state_map_census(year)
    states=stateDF['state']
    #- get the keyMap df
    print("getting the key maps")
    keyDF=cn.load_acskey_fields(mapdatapath,censustype='tract',kind=kind)
    keylist=keyDF['ACSKey']
    colmap={keyDF['ACSKey'].values[i]: keyDF['Variable'].values[i] for i in range(keyDF.shape[0])}
    #- get the tract full df
    tractDF=pd.DataFrame()
    for st in states:
        tractdf=cn.get_tract_data_acs(kind,st,year)
        tractdf=tractdf[keylist]
        newtractdf=tractdf.rename(colmap,axis=1)
        tractDF=tractDF.append(newtractdf)
    tractDF['TRACT']=tractDF['STATE']+tractDF['COUNTY']+tractDF['TRACT']
    tractDF['YEAR']=year
    return tractDF

def get_census_tract_data_static(kind,datapath):
    if kind not in ['demo','econ','educ','hshld']:
        print("kind not accepted for tract static data. use one from demo,econ or educ")
        sys.exit(0)
    years=np.arange(2009,2020)
    outDF=pd.DataFrame()
    for year in years:
        datafile=datapath+'/'+'Tract'+kind+str(year)[2:]+'.csv'
        print("processing data for year ", year," using datafile: ",datafile)
        dataDF=pd.read_csv(datafile,engine='python')
        if kind=='econ':
            fields=['TRACT','POP16','LT50KP','GT100P','GT150P','GT200P','MEDHINC','POVPERC']
            dataDF=dataDF[fields]
            dataDF.rename(columns={'POVPERC':'PovPerc'},inplace=True)
            numcols=['LT50KP','GT100P','GT150P','GT200P','MEDHINC','PovPerc']
        elif kind=='demo':
            fields=['TRACT','TOTPOP','WHIT','BLCK','AMIND','ASIA','HAWA','LATIN']
            dataDF=dataDF[fields]
            numcols=['TOTPOP','WHIT','BLCK','AMIND','ASIA','HAWA','LATIN']
        elif kind=='educ':
            fields=['TRACT','POP25','BACH','GRAD','BACHP']
            dataDF=dataDF[fields]
            dataDF.rename(columns={'BACH': 'BACHP','BACHP':'BachPlusP','GRAD':'GRADP'},inplace=True)
            numcols=['POP25','BACHP','GRADP','BachPlusP']
        elif kind=='hshld':
            fields=['tract','MalHse','MalHseSze','MALKID18','FemHse','FemHseSze','FEMKID18','MarCoup','MarSze','MARKID18','HsHldSze','AvFamSze','NonFamHse','NonFamSze','SameSex','TotFam','TotHse']
            dataDF=dataDF[fields]
            dataDF.rename(columns={'tract': 'TRACT'})
            numcols=['MalHse','MalHseSze','MALKID18','FemHse','FemHseSze','FEMKID18','MarCoup','MarSze','MARKID18','HsHldSze','AvFamSze','NonFamHse','NonFamSze','SameSex','TotFam','TotHse']
        #- convert all numeric columns to float
        dataDF[numcols] = dataDF[numcols].apply(pd.to_numeric, errors='coerce')
        dataDF['YEAR']=year-1 #- make consistent with to Census data as is
        print("Done with year:", year-1,"; Data dimensionality", dataDF.shape)
        outDF=outDF.append(dataDF)
    #- Sync the columns
    outDF=outDF.rename(columns={'Tract':'TRACT', 'tract':'TRACT','LT50KP': 'LT50P'})
    #- Add state from the map
    stateDF=cn.get_state_map_census('2018') #- hard code the most recent year
    statemap={stateDF['state'].values[i]: stateDF['NAME'].values[i] for i in range(stateDF.shape[0])}
    outDF['STATE']=outDF['TRACT'].astype(str).str[:-9].astype(int).map("{:02}".format)
    outDF.replace({"STATE":statemap},inplace=True)
    outDF.reset_index(inplace=True,drop=True)
    return outDF


def get_census_block_data(kind,year,mapdatapath,state=None):
    #- get statemap first
    if state is not None:
        print("Running for only given state: ",state)
        states=[state]
    else:
        print("Getting the state map")
        stateDF=cn.get_state_map_census(year)
        states=stateDF['state']
    #- get state name:
    stateDF=cn.get_state_map_census(year)
    statemap={stateDF['state'].values[i]: stateDF['NAME'].values[i] for i in range(stateDF.shape[0])}
    #- get the keyMap df
    print("getting the key maps")
    keyDF=cn.load_acskey_fields(mapdatapath,censustype='blkgrp',kind=kind)
    keylist=keyDF['ACSKey']
    colmap={keyDF['ACSKey'].values[i]: keyDF['Variable'].values[i] for i in range(keyDF.shape[0])}
    #- get the countyDF for all states
    ctyDF=cn.get_state_county_map_census(year)
    #- get the tract full df
    blockDF=pd.DataFrame()
    for st in states:
        #- get the county for this state:
        print("getting the counties for state ",st,": ", statemap[st])
        countyDF=ctyDF[ctyDF['state']==st]
        ctys=countyDF['county']
        print("Total number of counties: ", len(ctys))
        #- get the blockgroupd data for each counties
        for cty in ctys:
            blockdf=cn.get_blkgrp_data_acs(kind,st,cty,year)
            blockdf=blockdf[keylist]
            newblockdf=blockdf.rename(colmap,axis=1)
            #- add statename
            blockDF=blockDF.append(newblockdf)
        print("Finished with state: ", statemap[st])
    #blockDF['TRACT']=blockDF['STATE']+blockDF['COUNTY']+blockDF['TRACT']
    blockDF['BlkGrp']=blockDF['STATE']+blockDF['COUNTY']+blockDF['TRACT']+blockDF['BlkGrp']
    blockDF['YEAR']=year
    print("getting the final statistics")
    outDF=get_blk_stats(blockDF,kind)
    #- add statename from statemap
    outDF['STATE']=outDF['BlkGrp'].astype(str).str[:-10].astype(int).map("{:02}".format)
    outDF.replace({"STATE":statemap},inplace=True)
    outDF.reset_index(inplace=True,drop=True)
    return outDF

def get_blk_stats(df,kind):
    fnstr='get_blk_'+kind
    method_to_call=getattr(cn,fnstr)
    outDF=method_to_call(df)
    return outDF

def write_to_sqldb(df,outtable):
    from cdp_proc.load_sqldata import write_data_table
    server='129.119.63.219'
    dbname='CensusDB'
    uid=os.environ['HHUID']
    pwd=os.environ['HHPWD']
    write_data_table(df,server,dbname,outtable,uid,pwd)

def main(args):
    if args.censuslevel=='tract':
        if args.static_datapath is not None:
            print("Getting tract level data for all years")
            censusDF=get_census_tract_data_static(args.kind,args.static_datapath)
        else:
            censusDF=get_census_tract_data(args.kind,args.year,args.mapdatapath)
    elif args.censuslevel=='blkgrp':
        censusDF=get_census_block_data(args.kind,args.year,args.mapdatapath,args.state)
    if args.outcsv:
        censusDF.to_csv(args.outcsv,index=False)
    elif args.outsql:
        from cdp_proc.load_sqldata import write_data_table
        print("writing to output sql table")
        if args.censuslevel=='tract':
            sqltable='Tract'+args.kind
        elif args.censuslevel=='blkgrp':
            sqltable='BlkGrp'+args.kind
        write_to_sqldb(censusDF,sqltable)
        print("finished writing census data: ", args.censuslevel,": ", args.kind," -- to --> ", sqltable)
if __name__=='__main__':
    main(args)
