import pandas as pd
import numpy as np

def get_avg_commute(state,year,path):
    filename=state+'Commute'+year[2:]+'.csv'
    comm=pd.read_csv(datapath+'/'+filename)
    comm.drop(axis=0,index=0,inplace=True)
    comm=comm.reset_index()
    com_fields=[ii for ii in comm.columns.values if 'Commute' in ii and 'E' !=ii[-1] and 'N' not in ii[-1]]
    com_fields_i=[int(ii[7:]) for ii in com_fields]
    othfields=['BlkGrp','CommuteN']
    comm2=comm[othfields+com_fields].astype(int)
    comm2['AvgCommute']=sum(comm2[com_fields[ii]]*com_fields_i[ii] for ii in np.arange(len(com_fields)))/comm2['CommuteN']
    return comm2[['BlkGrp','AvgCommute']]




