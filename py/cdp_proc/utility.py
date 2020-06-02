import pandas as pd
import numpy as np

def stats_combine(companyfile,datadeffile):
    print("Running on company file")
    company=pd.read_csv(companyfile,engine='python')
    print("Company shape: ", company.shape)
    comp_stats=company.describe()
    nyear=len(set(company['year'].values))
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
    comb2['Counts annual avg']=comb2['count']/nyear
    #combine with the data def file 
    print("Finished stats for company. Reading the data def file")
    datadef=pd.read_excel(datadeffile)
    print("data def shape: ", datadef.shape)
    if 'KeyPick' in datadef.columns:
        datadef=datadef[['KeyPick','VarName','Variable description']]
        datadef['KeyPick'].fillna(0,inplace=True)
        datadef['KeyPick']=datadef['KeyPick'].astype(int)
    else:
        datadef=datadef[['VarName','Variable description']]
    comb2['VarName']=comb2.index
    print("Merging company with data definition")
    comb3=pd.merge(datadef,comb2,left_on='VarName',right_on='VarName',how='inner')
    #if outfile is not None:
    #    comb3.to_csv(outfile)
    #    print("wrote descriptive statistics for company to ",outfile)
    #- find the resids
    print("Total company fields: ",len(comb2['VarName'].values))
    print("Total datadef fields: ",len(datadef['VarName'].values)) 
    residcd=list(set(comb2['VarName'].values)-set(datadef['VarName'].values))
    rescdDF=pd.DataFrame({'company_fields': residcd})
    residdc=list(set(datadef['VarName'].values)-set(comb2['VarName'].values))
    resdcDF=pd.DataFrame({'VarName':residdc})
    #-combine residdc to desc
    resdcDF2=pd.merge(resdcDF,datadef,left_on='VarName',right_on='VarName',how='inner')
    return comb3,rescdDF,resdcDF2

def integrate_id_variables(companyfile,fieldfile):
    print("Running on company file")
    company=pd.read_csv(companyfile,engine='python')
    idfields=['NCARID','EIN','cdpid','TRGI','ORGName','sec_no','sector','YRFOUND','tzip','CBSA','year']   
    print("Company shape: ", company.shape)
    fieldDF=pd.read_csv(fieldfile)
    varfields=list(fieldDF['VarName'])
    allfields=idfields+varfields
    finalDF=company[allfields]
    print("final DF shape ",finalDF.shape )
    return finalDF

