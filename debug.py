# -*- coding: utf-8 -*-
"""
Created on Sat Sep  8 02:18:20 2018

@author: smccle01
"""

import random
import datetime
import pandas as pd
import seaborn as sns
import matplotlib as plt
import os
#from collections import defaultdict

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
    
def processBySelectExpression(df, quiet):
    
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
        
        sns.scatterplot(x=filtered.elapsedTimeSecs, 
                        y=filtered.bytesProcessed,
                        hue=filtered.host, 
                        #size=filtered.bytesScanned,
                        #sizes=(10, 200),
                        legend='brief', 
                        )
       
        
        plt.pyplot.show()
        
        del(filtered)
        
    
def processMetrics(metrics, quiet):
    
    #optionally override quiet..
    quiet = False
    verbose = not quiet
    
    metricsDir = "./metrics/"
    baseFilename = 'dbg-timing-metrics'
    metricsFilename = metricsDir + baseFilename + '.csv'
    saveFilename = metricsDir + baseFilename + "-save.csv"
    
    dfNew = pd.DataFrame(metrics)
    if verbose : print(dfNew) 
    
    if os.path.exists(metricsFilename):
        if verbose : print('old metrics file exists...')
        dfOld = pd.read_csv(metricsFilename)
        
        if verbose : print('concatenating new metrics...')
        frames = [dfOld, dfNew]
        dfCombined = pd.concat(frames, sort=True)
        
        
        if os.path.exists(saveFilename) :
            if verbose : print('removing old metrics file...')
            os.remove(saveFilename)
    
        if verbose : print("renaming old metrics file to '", saveFilename, "'", sep="")
        os.rename(metricsFilename, saveFilename)
        
        #save new metrics.csv file
        if verbose : print("saving new metrics file to '", metricsFilename, "'", sep="")
        dfCombined.to_csv(metricsFilename, index=False)
        
        if verbose : print('analysis of combined (new and saved) metric data...')
        processBySelectExpression(dfCombined, quiet)
    else:
        #save metrics.csv
        dfNew.to_csv(metricsFilename, index=False)
        
        if verbose : print('analysis of new metric data...')
        processBySelectExpression(dfNew, quiet)

#main    
if __name__ == "__main__" :

    quiet = True
    whichHosts = ['s3', 
                  'play', 
                  'm0', 
                  'z0'
                  ]
    TestDatasets = [ 
            { 'host': 's3', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv' },
            { 'host': 'play', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv'},
            { 'host': 'm0', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv' },
            { 'host': 'z0', 'bucket': 'airlines', 'object': 'DelayedFlights.csv' },
                 ]
    TestSelectExpressions = [ "select 1", "select 2", "select 3" ]
    
    metrics = list()
    for s in TestSelectExpressions :
            
        for d in TestDatasets :
            h = d['host']
            
            if h in whichHosts:
                print(">>> Querrying '", h, "'...", sep="")
                           
                startTime = datetime.datetime.now()
                b = d['bucket']
                o = d['object']
                #doSelect( "sjm-airlines", "DelayedFlights.csv", h, s, quiet)
                t = random.uniform(0.5,10.7)
                bp= random.uniform(10000.0,40000.0)
                bs= random.uniform(30000.0,80000.0)
                #time.sleep(t)
                endTime = startTime +  datetime.timedelta(seconds=t)  
                elapsedTime = printElapsedTime(startTime, endTime, quiet)
                elapsedTimeSecs = datetime.timedelta.total_seconds(elapsedTime)
                           
                metrics.append({"expression": s, 
                    'host' : h, 
                    'bucket': b, 
                    'object': o, 
                    'elapsedTimeDays': elapsedTime,
                    'elapsedTimeSecs': elapsedTimeSecs,
                    'bytesScanned': bs, 
                    'bytesProcessed': bp
                    })
            else: 
                print('----skipping host ', h)

    
    processMetrics(metrics, quiet)
   