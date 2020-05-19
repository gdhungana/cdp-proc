import pandas as pd
import numpy as np

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
    LT40_fields=['LT10','LT15','LT20','LT25','LT30','LT35','LT40']
    GT100_fields=['GT100','GT125','GT150','GT200']
    GT125_fields=['GT125','GT150','GT200']
    MidCI_fields=['LT45','LT50','LT60','LT55','LT75','LT100']
    MidCI2_fields=['LT45','LT50','LT55','LT60','LT75','LT100','LT125']
    #- convert the fields to float before calculation
    fulllist=list(set(LT40_fields+GT100_fields+GT125_fields+MidCI_fields+MidCI2_fields))
    print("Full list: ", fulllist)
    othfields=['BlkGrp','TotHse']
    econ=econ[fulllist+othfields].astype(int)
    #- Get the metrics
    econ['LT40p']=sum(econ[ii] for ii in LT40_fields)/econ['TotHse']
    econ['GT100p']=sum(econ[ii] for ii in GT100_fields)/econ['TotHse']
    econ['GT125p']=sum(econ[ii] for ii in GT125_fields)/econ['TotHse']
    econ['MidCIsP']=sum(econ[ii] for ii in MidCI_fields)/econ['TotHse']
    econ['MidCIsP2']=sum(econ[ii] for ii in MidCI2_fields)/econ['TotHse']
    return econ[['BlkGrp','TotHse','LT40p','GT100p','GT125p','MidCIsP']]

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


