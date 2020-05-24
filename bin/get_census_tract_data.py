import numpy as np
import pandas as pd
from cdp_proc import census as cn
import argparse
parser=argparse.ArgumentParser()
parser.add_argument("--censuslevel",required=True,help='the census level: tract or blkgrp')
parser.add_argument("--mapdatapath",default='../data',help='map to data path')
parser.add_argument("--kind", required=True,type=str, help='kind of information for the given census level')
parser.add_argument("--year", required=True,type=str, help='year to process data for')
parser.add_argument("--state",default=None type=str,help='state to run the block group export on')
parser.add_argumetn("--outcsv", required=False, help='output csv if given')
parser.add_argument("--outsql", store_action=False, help='switch to write to a sql, db table name hardcoded')
args = parser.parse_args()

def get_census_tract_data(kind,year,mapdatapath):
    #- get statemap first
    print("Getting the state map")
    stateDF=cn.get_state_map_census()
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
        
        
def get_census_block_data(kind,year,mapdatapath,state=None):
    #- get statemap first
    if state is not None:
        print("Running for only given state: ",st)
        states=[state]
    else:
        print("Getting the state map")
        stateDF=cn.get_state_map_census()
        states=stateDF['state']
    #- get the keyMap df
    print("getting the key maps")
    keyDF=cn.load_acskey_fields(mapdatapath,censustype='blkgrp',kind=kind)
    keylist=keyDF['ACSKey']
    colmap={keyDF['ACSKey'].values[i]: keyDF['Variable'].values[i] for i in range(keyDF.shape[0])}
    #- get the tract full df
    blockDF=pd.DataFrame()
    for st in states:
        #- get the county for this state:
        print("getting the counties for state ", st)
        countyDF=cn.get_state_county_map_census(st)
        ctys=countyDF['county']
        print("Total number of counties: ", len(ctys))
        #- get the blockgroupd data for each counties
        for cty in ctys:
            blockdf=cn.get_blkgrp_data_acs(kind,st,cty,year)
            blockdf=blockdf[keylist]
            newblockdf=blockdf.rename(colmap,axis=1)
            blockDF=blockDF.append(newblockdf)
    #blockDF['TRACT']=blockDF['STATE']+blockDF['COUNTY']+blockDF['TRACT']
    blockDF['BlkGrp']=blockDF['STATE']+blockDF['COUNTY']+blockDF['TRACT']+blockDF['BlkGrp']
    blockDF['YEAR']=year
    return blockDF

def main(args):
    if args.censuslevel=='tract':
        censusDF=get_census_tract_data(args.kind,args.year,args.mapdatapath)
    elif args.censuslevel='blkgrp':
        censusDF=get_census_block_data((args.kind,args.year,args.mapdatapath,args.state)
    if args.outcsv:
        censusDF.to_csv(args.outcsv,index=False)
    elif args.outsql:
        print("writing to output sql table")
   

if __name__=='__main__':
    main(args)
