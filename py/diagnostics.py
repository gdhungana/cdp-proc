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
