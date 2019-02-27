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
import sqlparse#
#import os

#from collections import defaultdict
#import time

#defne locatin for the dataset(s)... {host, bucket, object}
TestDatasets = [ 
        { 'host': 's3', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv' },
        { 'host': 'play', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv'},
        { 'host': 'm0', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv' },
        { 'host': 'm3', 'bucket': 'sjm-airlines', 'object': 'DelayedFlights.csv' },
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
    
    endpoint = mc.getMinioHostInfo().getURL(hostName)
    secureFlag = ("https://" in endpoint)
    print(secureFlag)
    
    #setup client, use mc.getMinioHostInfo().finctions to look up URL, accessKey, and secretKey
    s3 = boto3.resource('s3',
                          endpoint_url=endpoint,
                          aws_access_key_id=mc.getMinioHostInfo().getAccessKey(hostName),
                          aws_secret_access_key=mc.getMinioHostInfo().getSecretKey(hostName),verify =False
                      #    is_secure=secureFlag
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
    
    endpoint = mc.getMinioHostInfo().getURL(hostName)
    secureFlag = ("https://" in endpoint)
    print(secureFlag)
    
    #setup client, use mc.getMinioHostInfo().finctions to look up URL, accessKey, and secretKey
    s3 = boto3.client('s3',
                      endpoint_url=endpoint,
                      aws_access_key_id=mc.getMinioHostInfo().getAccessKey(hostName),
                      aws_secret_access_key=mc.getMinioHostInfo().getSecretKey(hostName),
                      #is_secure=secureFlag,
                      verify=False,
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
                print(record, end="")
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
        

    
def doSelectShowPayload(bucketName, objectName, hostName, selectExpression):
    
    #startTime = datetime.datetime.now()
    
    #setup client, use mc.getMinioHostInfo().finctions to look up URL, accessKey, and secretKey
    s3 = boto3.client('s3',
                      endpoint_url=mc.getMinioHostInfo().getURL(hostName),
                      aws_access_key_id=mc.getMinioHostInfo().getAccessKey(hostName),
                      aws_secret_access_key=mc.getMinioHostInfo().getSecretKey(hostName),
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
        print(event)
        if 'Records' in event:
            record = event['Records']['Payload'].decode('utf-8')
            print(record, end="")
        elif 'Stats' in event:
            statsDetails = event['Stats']['Details']
            bs = statsDetails['BytesScanned']
            bp = statsDetails['BytesProcessed']
            
            print("Stats details bytesScanned: ", bs)
            print("Stats details bytesProcessed: ", bp)
                
    #if quiet: 
    #    print("**DONE - (Output not echoed!)**")
        
    #endTime = datetime.datetime.now() 
    #elapsedTime = printElapsedTime(startTime, endTime, quiet)
    #elapsedTimeSecs = datetime.timedelta.total_seconds(elapsedTime)
    
        
#various print functions for fomatted output

def printElapsedTime( startTime, endTime, quiet):
    if not quiet: 
        print("StartTime: ", startTime)
        print("EndTime: ", endTime)
    
    elapsedTime = endTime - startTime

    print("ElapsedTime: ", elapsedTime)
    return(elapsedTime)
    
def printSelectExpression(selectExpression):
    
    #n = len(selectExpression)
    n= 60
    dashes = "-" * n
    
    print(dashes)
    print(sqlparse.format(selectExpression,reindent=True, keyword_case='upper'))
    print(dashes)
    
def printHostInfo( hostName):
    print("Host '", hostName, "' (", mc.getMinioHostInfo().getURL(hostName), ")", sep="" )

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
    s = "select * from S3Object s where s.Origin = 'SMF' AND s.Dest = 'ATL' limit  10"
    #s = "select count(*) from S3Object s where s.WeatherDelay <> '0' and s.WeatherDelay <> ''"
    #s = "select s.UniqueCarrier from S3Object s where s.WeatherDelay <> '0.0' and s.WeatherDelay <> '' limit 5000"
    printSelectExpression(s)
    
    quiet = False
    skipS3 = True
    if not skipS3 :
        h = "s3"
        #printDatasetInfo("sjm-airlines", "DelayedFlights.csv", h)
        printHostInfo(h)
        doSelect( "sjm-airlines", "DelayedFlights.csv", h, s, quiet)
        print()

    skipPlay = True
    if not skipPlay :
        h = "play"
        printHostInfo(h)
        doSelect( "sjm-airlines", "DelayedFlights.csv", h, s, quiet)
        print()
                   
    skipZ0 = True
    if not skipZ0 :
        h = "z0"
        printHostInfo(h)
        doSelect( "airlines", "DelayedFlights.csv", h, s, quiet)
        print()
    
    skipM0 = True
    if not skipM0 : 
        h = "m0"
        printHostInfo(h)
        doSelect( "sjm-airlines", "DelayedFlights.csv", h, s, quiet)  
        print()
        
    skipC0 = True
    if not skipC0 :
        h = "c0"
        printHostInfo(h)
        doSelect( "badscooter", "DelayedFlights.csv", h, s, quiet)
        print()
        
    skipC1 = False
    if not skipC1 :
        h = "c1"
        printHostInfo(h)
        doSelect( "airlines", "DelayedFlights.csv", h, s, quiet)
        print()
        
    skipC2 = True
    if not skipC2 :
        h = "c2"
        printHostInfo(h)
        doSelect( "airlines", "DelayedFlights.csv", h, s, quiet)
        print()
        
    print()
                
def iterateThroughTests(whichHosts):

    #optionally override quiet..
    #quiet = True

    for s in TestSelectExpressions :
        printSelectExpression(s)
        
        for t in TestDatasets :
            h = t['host']
            if h in whichHosts:
                alias = mc.getMinioHostInfo().getAlias(h)
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
                        legend=False, 
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


def showHarshaPayloadBug():
    
    s = "select * from S3Object s where s.Origin = 'SMF' AND s.Dest = 'ATL' limit 100"
    printSelectExpression(s)
    
    h = "s3"
    #printDatasetInfo("sjm-airlines", "DelayedFlights.csv", h)
    printHostInfo(h)
    doSelectShowPayload( "sjm-airlines", "DelayedFlights.csv", h, s)
    print()
    
    h = "play"
    #printDatasetInfo("sjm-airlines", "DelayedFlights.csv", h)
    printHostInfo(h)
    doSelectShowPayload( "sjm-airlines", "DelayedFlights.csv", h, s)
    print()
    
        
#main    
if __name__ == "__main__" :
    #os.environ['SSL_CERT_FILE'] = 'prgx_ca.pem'
    
    #supress printing extra information (eg: quiet supressing printing data returned 
    #by the slect query against th data set)
    quiet = True
    
    metrics = list()
    
    #initialize the getMinioHostInfo() class
    mc.getMinioHostInfo()
    
    
    #True to test individual select calls, False to skip
    if True:
        testIndividualSelectCalls()
        
        #showGraphs = True
        #processMetrics( metrics, quiet, showGraphs)
        
    #True to test all the select statemetns against all the hosts, False to skip
    if False:
        whichHosts = [
                'c1'
                ]
        iterateThroughTests(whichHosts)
    
        showGraphs = True
        processMetrics( metrics, quiet, showGraphs)
        
    if False :
        showHarshaPayloadBug()
       
    
    print("All DONE!")

   
       