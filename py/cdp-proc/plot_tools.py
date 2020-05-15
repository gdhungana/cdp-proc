import seaborn as sns
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def bar_plot_hh(hhcountfile):
    data=pd.read_csv(hhcountfile,delim_whitespace=1)
    xx=np.arange(data.shape[0])
    fig=plt.figure(figsize=(12,6))
    plt.bar(xx,data['HH_count'],align='center',alpha=0.8,color='b')
    plt.xticks(xx, data['ST'],fontsize=10,rotation=45)
    if data.shape[1]>2:
        plt.bar(xx,data['Org_count'],align='center',alpha=0.4,color='r')
        plt.title("HH and Org Count by State")
    else:
        plt.title("HH Count by State")
    plt.xlabel(r'$State$')
    plt.ylabel(r'$Frequency$')
    plt.yscale('log')
    plt.show()   

