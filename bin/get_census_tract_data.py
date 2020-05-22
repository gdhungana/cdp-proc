import numpy as np
import pandas as pd
from cdp-process.census import load_acskey_fields,get_tract_data_acs,get_state_map_census,get_state_county_map_census,get_blkgrp_data_acs

def get_census_tract_data(kind,year,mapdatapath):
    #- get statemap first
    print("Getting the state map")
    stateDF=get_state_map_census()
    states=stateDF['state']
    #- get the keyMap df
    print("getting the key maps")
    keyDF=load_acskey_fields(censustype='tract',kind=kind,datapath=mapdatapath)
    keylist=keyDF['ACSKey']
    colmap={keyDF['ACSKey'].values[i]: keyDF['Variable'].values[i] for i in range(keyDF.shape[0])}
    #- get the tract full df
    tractDF=pd.DataFrame()
    for st in states:
        tractdf=get_tract_data_acs(kind,st,year)
        tractdf=tractdf[keylist]
        newtractdf=tractdf.rename(colmap,axis=1)
        tractDF=tractDF.append(newtractdf)
    tractDF['TRACT']=tractDF['STATE']+tractDF['COUNTY']+tractDF['TRACT']
    tractDF['YEAR']=year
    return tractDF
        
        
def get_census_block_data(kind,year,mapdatapath,state=None):
    #- get statemap first
    if state is not None:
        states=[state]
    else:
        print("Getting the state map")
        stateDF=get_state_map_census()
        states=stateDF['state']
    #- get the keyMap df
    print("getting the key maps")
    keyDF=load_acskey_fields(censustype='blkgrp',kind=kind,datapath=mapdatapath)
    keylist=keyDF['ACSKey']
    colmap={keyDF['ACSKey'].values[i]: keyDF['Variable'].values[i] for i in range(keyDF.shape[0])}
    #- get the tract full df
    blockDF=pd.DataFrame()
    for st in states:
        #- get the county for this state:
        print("getting the counties for state ", st)
        countyDF=get_state_county_map_census(st)
        ctys=countyDF['county']
        print("Total number of counties: ", len(ctys))
        #- get the blockgroupd data for each counties
        for cty in ctys:
            blockdf=get_blkgrp_data_acs(kind,st,cty,year)
            blockdf=blockdf[keylist]
            newblockdf=blockdf.rename(colmap,axis=1)
            blockDF=blockDF.append(newblockdf)
    blockDF['TRACT']=blockDF['STATE']+blockDF['COUNTY']+blockDF['TRACT']
    blockDF['BlkGrp']=blockDF['STATE']+blockDF['COUNTY']+blockDF['TRACT']+blockDF['BlkGrp']
    blockDF['YEAR']=year
    return blockDF

