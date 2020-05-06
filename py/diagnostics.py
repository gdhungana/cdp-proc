import numpy as np
import pandas as pd

def get_diffs_eff(eff1file,eff2file,year1,year2):
    """
    returns df2-df1, and df1-df2 for eff tables
    """
    print("Checking data from ", eff1file)
    eff1=pd.read_csv(eff1file,engine='python') 
    eff1=eff1[eff1['Year']==year1]
    print("Shape of Eff1:", eff1.shape) 
    ef1=eff1[['cdpid','Year','NCARID','TOTOFFRCDRank']].dropna(subset=['TOTOFFRCDRank']) 
    set1=set(ef1['cdpid'].values)    

    print("Checking data from ", eff2file)
    eff2=pd.read_csv(eff2file,engine='python')                                                       
    eff2=eff2[eff2['Year']==year2]
    print("Shape of Eff2:", eff2.shape)
    ef2=eff2[['cdpid','Year','NCARID','TOTOFFRCDRank']].dropna(subset=['TOTOFFRCDRank'])  
    set2=set(ef2['cdpid'].values)
    
    comp12=set1-set2
    comp21=set2-set1
    return comp12,comp21 #- set1-set2, set2-set1

def map_to_dashOrgs(cdpids,dashOrgsfile,outfile=None):
    """
    cdpids: list of updated cdpids
    Need to get ORGID, ORGNAME, Password, Email, CONTFNAME, CONTLNAME,NCARID,sec_no, Dash18, Dash19
    """
    cdpDF=pd.DataFrame(cdpids,columns=['upCDPID'])
    dashOrgs=pd.read_csv(dashOrgsfile,engine='python')
    dashresid=pd.merge(dashOrgs,cdpDF,left_on='OrgID',right_on='upCDPID',how='inner')
    dashresid.drop("upCDPID",axis=1,inplace=True)
    if outfile is not None:
        dashresid.to_csv(outfile,index=0)
    return dashresid

