# -*- coding: utf-8 -*-
"""
Created on Wed Sep 12 18:00:45 2018

@author: smccle01
"""

#import datetime
import miniohostinfo as mc
import pandas as pd
import seaborn as sns
import os
import matplotlib as plt
#from collections import defaultdict
#import time

def printElapsedTime( startTime, endTime, quiet):
    if not quiet: 
        print("StartTime: ", startTime)
        print("EndTime: ", endTime)
    
    elapsedTime = endTime - startTime

    print("ElapsedTime: ", elapsedTime)
    return(elapsedTime)
    
def printSelectExpression(selectExpression):
    dashes = "-" * len(selectExpression)
    print(dashes)
    print(selectExpression)
    print(dashes)

def processBySelectExpression(df, quiet, showGraphs):
    
    #optionally override quiet..
    quiet = False
    verbose = not quiet   
    
    selectionExpressions = df.get('expression').unique()
    for e in selectionExpressions :
        printSelectExpression(e)
        
        filtered = pd.DataFrame(df.loc[df['expression']==e])
        if verbose : 
            print(filtered.describe())
        
        
        sns.set(style="ticks", palette="pastel")
        sns.boxplot(x=filtered.host, y=filtered.elapsedTimeSecs)
        plt.pyplot.show()
        
        sns.set(style="ticks", palette="pastel")
        sns.swarmplot(x=filtered.host, y=filtered.elapsedTimeSecs)
        plt.pyplot.show()
        
        sns.scatterplot(x=filtered.bytesProcessed,
                        y=filtered.elapsedTimeSecs, 
                        hue=filtered.host, 
                        size=filtered.bytesScanned,
                        sizes=(10, 200),
                        #legend='brief', 
                        legend=False,
                        )
        plt.pyplot.show()
        
        del(filtered)
        
    
def processMetricsFile(quiet, showGraphs):
    
    #optionally override quiet..
    quiet = False
    verbose = not quiet
    
    metricsDir = "./metrics/"
    # for debugging code change baseFilename to 'dbg-timing-metrics'
    baseFilename = 'timing-metrics'
    metricsFilename = metricsDir + baseFilename + '.csv'
    #saveFilename = metricsDir + baseFilename + "-save.csv"

    if os.path.exists(metricsFilename):
        if verbose : print('old metrics file exists...')
        dfOld = pd.read_csv(metricsFilename)
        
        processBySelectExpression(dfOld, quiet, showGraphs)
    else:
        print("****ERROR: metrics file '", metricsFilename, "' does not exist!", sep="")

        
#main    
if __name__ == "__main__" :
    
    #supress printing extra information (eg: quiet supressing printing data returned 
    #by the slect query against th data set)
    quiet = True
    
    showGraphs = True
    processMetricsFile(quiet, showGraphs)
    
    print("All DONE!")

   
       