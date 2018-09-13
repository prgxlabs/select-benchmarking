# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import json
import subprocess
import pprint
from collections import defaultdict

def getMinioHostInfo():
    debug = 0
    p = subprocess.run(['mc', 'config', 'host', 'list', '--json'], stdout=subprocess.PIPE)
    
    hostDict = defaultdict(list)
    lines = p.stdout.decode('utf-8').split('\n')
    for line in lines:
        if debug >= 3: 
            print(">>>line")
            print(line)
            print("<<<line")
    
        #skip blank lines
        if line == "":
            break
          
        hostInfo = json.loads(line)
        if debug >= 2: 
            print(">>>JSON")
            pprint.pprint(hostInfo)
            print("<<<JSON")

        hostDict[hostInfo['alias']]= hostInfo

    return hostDict

def getURL(hostName, hostDict): 
    debug = 0 
    try:
        h = hostDict.get(hostName)
        if h.get('alias') != hostName:
            print('Oh NO!... alias does not match hostName', h.get('alias'), hostName)
            
        try:
            url = h.get('URL')
            return(url)
        except:
            if debug > 0:
                print(h)
                print("error... no 'URL' for '", hostName,"'")
            return("")
    except:
        if debug > 0:
            print("error... host '", hostName, "' not found in dictionary'")
        return("")
            
def getAccessKey(hostName, hostDict): 
    debug = 0 
    try:
        h = hostDict.get(hostName)
        if h.get('alias') != hostName:
            print('Oh NO!... alias does not match hostName', h.get('alias'), hostName)
            
        try:
            accessKey = h.get('accessKey')
            return(accessKey)
        except:
            if debug > 0:
                print(h)
                print("error... no 'accessKey' for '", hostName,"'")
            return("")
    except:
        if debug > 0:
            print("error... host '", hostName, "' not found in dictionary'")
        return("")
        
def getSecretKey(hostName, hostDict): 
    debug = 0 
    try:
        h = hostDict.get(hostName)
        if h.get('alias') != hostName:
            print('Oh NO!... alias does not match hostName', h.get('alias'), hostName)
            
        try:
            secretKey = h.get('secretKey')
            return(secretKey)
        except:
            if debug > 0:
                print(h)
                print("error... no 'secretKey' for '", hostName,"'")
            return("")
    except:
        if debug > 0:
            print("error... host '", hostName, "' not found in dictionary'")
        return("")
    
def getAlias(hostName, hostDict): 
    debug = 0 
    try:
        h = hostDict.get(hostName)
        alias = h.get('alias')
        if alias != hostName:
            print('Oh NO!... alias does not match hostName', h.get('alias'), hostName)
        return(alias)

    except:
        if debug > 0:
            print("error... host '", hostName, "' not found in dictionary'")
        return("")
        
def getStatus(hostName, hostDict): 
    debug = 0 
    try:
        h = hostDict.get(hostName)
        if h.get('alias') != hostName:
            print('Oh NO!... alias does not match hostName', h.get('alias'), hostName)
            
        try:
            status = h.get('status')
            return(status)
        except:
            if debug > 0:
                print(h)
                print("error... no 'status' for '", hostName,"'")
            return("")
    except:
        if debug > 0:
            print("error... host '", hostName, "' not found in dictionary'")
        return("")
        
def getAPI(hostName, hostDict): 
    debug = 0 
    try:
        h = hostDict.get(hostName)
        if h.get('alias') != hostName:
            print('Oh NO!... alias does not match hostName', h.get('alias'), hostName)
            
        try:
            api = h.get('api')
            return(api)
        except:
            if debug > 0:
                print(h)
                print("error... no 'api' for '", hostName,"'")
            return("")
    except:
        if debug > 0:
            print("error... host '", hostName, "' not found in dictionary'")
        return("")
        
if __name__ == "__main__" :
    
    quiet = True
    
    hostDict = getMinioHostInfo()
    #print(hostDict)
    
    searchlist = ['s3', 'play', 'm0', 'm7', 'z0', "local"]
    
    for h in searchlist :
        print('Host config information for ', h, sep="")
        alias = getAlias(h, hostDict)
        if alias != "": 
            print("   status:", getStatus(h, hostDict))
            print("   alias:", getAlias(h, hostDict))
            print("   URL: ", getURL(h, hostDict))
            print("   accessKey:", getAccessKey(h, hostDict))
            print("   secretKey:", getSecretKey(h, hostDict))
            print("   api: ", getAPI(h, hostDict))
        else:
            print("-- No host matching '", h, "' found is configured.", sep="")
        print()
