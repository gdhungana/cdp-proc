import pandas as pd
import numpy as np

def stats_combine(companyfile,datadeffile):
    company=pd.read_csv(companyfile,engine='python')
    comp_stats=company.describe()
    #- add missing ones
    des2 = company.isnull().sum().to_frame(name = 'missing count').T
    comb=pd.concat([comp_stats,des2],axis=0)
    #- drop the categorical
    categ=list(set(company.columns.values)-set(comp_stats.columns.values))
    comb.drop(categ,axis=1,inplace=True) 
    #- get the description for categ
    companycat=company[categ]
    des_cat = companycat.isnull().sum().to_frame(name = 'missing count')
    des_cat['count'] = company.shape[0]-des_cat['missing count']
    comb2=pd.concat([comb,des_cat.T],axis=1).T
    comb2['Counts annual avg']=comb2['count']/5
    if outfile is not None:
        comb2.to_csv(outfile)
        print("wrote descriptive statistics for company to ",outfile)
    return comb2


