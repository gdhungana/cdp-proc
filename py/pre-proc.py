import numpy as np
import pandas as pd
from cdp-prop.load_sqldata import load_data,write_data_table

def clean_trg_map(trgmapfile):
    trgmap=pd.read_csv(trgmapfile)
    trgmap['CITY']=trgmap['CITY'].str.upper()
    trgmap['CNTYNM']=trgmap['CNTYNM'].str.upper()
    #- clean up any leading or trailing white spaces on string fields
    strfields=['ORGName','ADDRESS','CITY','STATE','CNTYNM','ZIP9','NetworkCode','NetworkName']
    for col in strfields:
        trgmap[col]=trgmap[col].str.strip()
    return trgmap #- write_data_table will write to the table

def comb_hhdata_org(cnxn,addgenre=False,addsec_no=False):
   sqltrg="select * from TRGMap"
   trgmap=load_data(cnxn,sqltrg)
   print("TRG MAP orgids: ", len(set(trgmap['OrgID'].values)))
   sqlOrg="SELECT OrgID,AnnualRevenue,AnnualRevenueYear,PostalCode from Organization"
   organization=load_data(cnxn,sqlOrg)
   print("organization orgids: ", len(set(trgmap['OrgID'].values)))
   trg_org=pd.merge(trgmap,organization,left_on='OrgID',right_on='OrgID',how='inner')
   print("trg_org orgids: ", len(set(trgmap['OrgID'].values)))
   #- combining Orggenre 
   if addgenre:
       print("add Org Genre")
       sqlGen="select distinct OrgID, first_value(Genre) over(partition by OrgId order by Genre ASC) as TRG_Genre from OrgGenre"
       orggen=load_data(cnxn,sqlGen)
       print("Genre available for Orgs: ", len(set(orggen['OrgID'].values)))
       trg_org=pd.merge(trg_org,orggen,left_on='OrgID',right_on='OrgID', how='left')
   if addsec_no:
       print("add sec_no from Orgmap")
       sqlorgmap="select distinct cast(TRGI as int) as TRGI, sec_no from orgmap where TRGI is not NULL"
       orgmap=load_data(cnxn,sqlorgmap)
       print("TRGI sec_no given for ", len(set(orgmap['TRGI'].values)))
       trg_org=pd.merge(trg_org,orgmap,left_on='OrgID',right_on='TRGI', how='left')
       trg_org.drop('TRGI',axis=1,inplace=True)    
   print("Shape of final TRG ORG integ: ",trg_org.shape) 
   return trg_org


def clean_household(cnxn):
    sqlhh="select distinct HouseholdID,City,replace(State,'[^\W ]','') as state,PostalCode from Household_20200501 where State is not NULL"
    hhdf=load_data(cnxn,sqlhh)
    hhdf['State']==hhdf['State'].str.upper()
    print("Distinct hh:", len(set(hhdf['HouseholdID'].values)))
    #- Now map full State to State Code
    sqlfips="select * from FipsstateMap"
    fips=load_data(cnxn,sqlfips)
    #- join fips
    print("Joining hh with fipsstatecode")
    hhfips=pd.merge(hhdf,fips,left_on='state',right_on='state',how='left')
    print("Distinct hhfips hh:", len(set(hhfips['HouseholdID'].values)))
    
