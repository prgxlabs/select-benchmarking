# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

#!/usr/bin/env/env python3
import boto3
import datetime
import miniohostinfo as mc
import pandas as pd
import seaborn as sns
import os
import matplotlib as plt
#from collections import defaultdict
#import time

#defne locatin for the dataset(s)... {host, bucket, object}
TestDatasets = [ 
        { 'host': 's3', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv' },
        { 'host': 'play', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv'},
        { 'host': 'm0', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv' },
        { 'host': 'z0', 'bucket': 'airlines', 'object': 'DelayedFlights.csv' },
             ]

#list of select expressions which will be tested...
TestSelectExpressions = [
        "select count(*) from S3Object s where s.UniqueCarrier = 'WN'",
        "select * from S3Object s where s.Origin = 'SMF' AND s.Dest = 'ATL' limit  10",
        "select count(*) from S3Object s where s.UniqueCarrier = 'WN' and s.WeatherDelay <> '0.0' and s.WeatherDelay <> '' ",
        "select s.Origin , s.WeatherDelay from S3Object s where s.UniqueCarrier = 'WN' and s.WeatherDelay <> '0.0' and s.WeatherDelay <> '' limit 50",
        "select s.UniqueCarrier from S3Object s where s.WeatherDelay <> '0.0' and s.WeatherDelay <> '' limit 5000",
        "select * from S3Object s where s.Origin = 'SMF' AND s.UniqueCarrier = 'WN'  limit  10",
        ]
    
def lookupBucket(hostName):
    for t in TestDatasets:
        if t['host'] == hostName:
            return(t['bucket'])
    return('**NOT FOUND**')

def lookupObjectName(hostName):
    for t in TestDatasets:
        if t['host'] == hostName:
            return(t['object'])
    return('**NOT FOUND**')
    
def getColumntHeaders(bucketName, objectName, hostName, delim=","):
    
    #setup client, use mc.finctions to look up URL, accessKey, and secretKey
    s3 = boto3.resource('s3',
                          endpoint_url=mc.getURL(hostName, hostDict),
                          aws_access_key_id=mc.getAccessKey(hostName, hostDict),
                          aws_secret_access_key=mc.getSecretKey(hostName, hostDict)
                        )
    #setup object (o) based on bucketName and objectName
    o = s3.Object(bucketName, objectName)

    #read first line (returns bytes-like object), decode as 'utf-8'...
    #split indo list of column names using delim
    columns = o.get()['Body']._raw_stream.readline().decode('utf-8').split(delim)
    
    #return linst of column header names
    return(columns)

    
def doSelect(bucketName, objectName, hostName, selectExpression, quiet):
    
    startTime = datetime.datetime.now()
    
    #setup client, use mc.finctions to look up URL, accessKey, and secretKey
    s3 = boto3.client('s3',
                      endpoint_url=mc.getURL(hostName, hostDict),
                      aws_access_key_id=mc.getAccessKey(hostName, hostDict),
                      aws_secret_access_key=mc.getSecretKey(hostName, hostDict),
                      region_name='us-east-1')
        
    
    #make the select_object_content call... returns a stream
    #TODO: assumes dataset is CSV and is not compressed... should relax this
    eventStream = s3.select_object_content(
                            Bucket=bucketName,
                            Key=objectName,
                            ExpressionType='SQL',
                            Expression=selectExpression,
                            InputSerialization={
                                                'CSV': {
                                                        "FileHeaderInfo": "USE",
                                                        },
                                                'CompressionType': 'NONE',
                                                },
                            OutputSerialization={'CSV': {}},
                            )
    
    #iterate through the response (eventStream)
    for event in eventStream['Payload']:
        #debugging code - totally messes up output
        #print(event)
        if 'Records' in event:
            record = event['Records']['Payload'].decode('utf-8')
            if not quiet :
                print(record)
        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            bs = statsDetails['BytesScanned']
            bp = statsDetails['BytesProcessed']
            if not quiet : 
                print("Stats details bytesScanned: ", bs)
                print("Stats details bytesProcessed: ", bp)
                
    if quiet: 
        print("**DONE - (Output not echoed!)**")
        
    endTime = datetime.datetime.now() 
    elapsedTime = printElapsedTime(startTime, endTime, quiet)
    elapsedTimeSecs = datetime.timedelta.total_seconds(elapsedTime)
    
    metrics.append({"expression": selectExpression, 
                    'host' : hostName, 
                    'bucket': bucketName, 
                    'object': objectName, 
                    'elapsedTimeDays': elapsedTime,
                    'elapsedTimeSecs': elapsedTimeSecs,
                    'bytesScanned': bs, 
                    'bytesProcessed': bp
                    })
        

#various print functions for fomatted output

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
    
def printHostInfo( hostName):
    print("Host '", hostName, "' (", mc.getURL(hostName, hostDict), ")", sep="" )

def printColumnHeaders(bucketName, objectName, hostName, onePerLine=False):
    columnHeaders = getColumntHeaders(bucketName, objectName, hostName) 
    
    if onePerLine :
        i = 0
        for c in columnHeaders:
            i +=1
            print("Column ", i, ": '", c, "'", sep="")
    else : 
        print(columnHeaders)
    
def printDatasetInfo( bucketName, objectName, hostName, printCols=True):
    printHostInfo(hostName)
    if printCols:
        onePerLine = True
        printColumnHeaders(bucketName, objectName, hostName, onePerLine)
        
def testIndividualSelectCalls():
    #s = "select s.UniqueCarrier from S3Object s where s.WeatherDelay <> '0.0' and s.WeatherDelay <> '' limit 10"
    s = "select s.UniqueCarrier from S3Object s where s.WeatherDelay <> '0.0' and s.WeatherDelay <> '' limit 5000"
    printSelectExpression(s)
    
    h = "s3"
    #printDatasetInfo("sjm-airlines", "DelayedFlights.csv", h)
    printHostInfo(h)
    doSelect( "sjm-airlines", "DelayedFlights.csv", h, s, quiet)
    print()

    h = "play"
    printHostInfo(h)
    doSelect( "sjm-airlines", "DelayedFlights.csv", h, s, quiet)
    print()
                   
    h = "z0"
    printHostInfo(h)
    doSelect( "airlines", "DelayedFlights.csv", h, s, quiet)
    print()
    
    h = "m0"
    printHostInfo(h)
    doSelect( "sjm-airlines", "DelayedFlights.csv", h, s, quiet)    
    print()
                
def iterateThroughTests(whichHosts):

    #optionally override quiet..
    #quiet = True

    for s in TestSelectExpressions :
        printSelectExpression(s)
        
        for t in TestDatasets :
            h = t['host']
            if h in whichHosts:
                alias = mc.getAlias(h, hostDict)
                if alias != "": 
                    print(">>> Querrying '", h, "'...", sep="")
                    doSelect( t['bucket'], t['object'], h, s, quiet)
                else:
                    print("ERROR: No host matching '", h, "' found is configured.", sep="")
    print()
    
def processBySelectExpression(df, quiet, showGraphs):
    
    #optionally override quiet..
    quiet = False
    verbose = not quiet
    
    if not showGraphs :
        if not quiet : print("supressing graphs...")
        
        #just return... don't do the work below to render the graphs
        return()
        
    
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
        
    
def processMetrics(metrics, quiet, showGraphs):
    
    #optionally override quiet..
    quiet = False
    verbose = not quiet
    
    metricsDir = "./metrics/"
    # for debugging code change baseFilename to 'dbg-timing-metrics'
    baseFilename = 'timing-metrics'
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
        processBySelectExpression(dfCombined, quiet, showGraphs)
    else:
        #save metrics.csv
        dfNew.to_csv(metricsFilename, index=False)
        
        if verbose : print('analysis of new metric data...')
        processBySelectExpression(dfNew, quiet, showGraphs) 

        
#main    
if __name__ == "__main__" :
    
    #supress printing extra information (eg: quiet supressing printing data returned 
    #by the slect query against th data set)
    quiet = True
    
    metrics = list()
    
    #create dictionary of information about hosts configured in minio client (mc) config file. 
    #dictionary will cotain all info (url, accessKey, secretKey) etc
    hostDict = mc.getMinioHostInfo()
    
    #True to test individual select calls, False to skip
    if True:
        testIndividualSelectCalls()
        
    #True to test all the select statemetns against all the hosts, False to skip
    if False:
        whichHosts = [
                's3', 
                'play', 
                'm0',
                'z0'
                ]
        iterateThroughTests(whichHosts)
    
    showGraphs = False
    processMetrics( metrics, quiet, showGraphs)
    
    print("All DONE!")

   
       