# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""
#import fire
import json
import subprocess
import pprint
from collections import defaultdict

class getMinioHostInfo:
    

    hostDict = defaultdict

    def __init__(self):
        debug = 0
        p = subprocess.run(['mc', 'config', 'host', 'list', '--json'], stdout=subprocess.PIPE)
        
        self.hostDict = defaultdict(list)
        lines = p.stdout.decode('utf-8').split('\n')
        print(lines)
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
    
            self.hostDict[hostInfo['alias']]= hostInfo
        return 
    
    def getURL(self, hostName): 
        debug = 0 
        try:
            h = self.hostDict.get(hostName)
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
                
    def getAccessKey(self, hostName): 
        debug = 0 
        try:
            h = self.hostDict.get(hostName)
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
            
    def getSecretKey(self, hostName): 
        debug = 0 
        try:
            h = self.hostDict.get(hostName)
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
        
    def getAlias(self, hostName): 
        debug = 0 
        try:
            h = self.hostDict.get(hostName)
            alias = h.get('alias')
            if alias != hostName:
                print('Oh NO!... alias does not match hostName', h.get('alias'), hostName)
            return(alias)
    
        except:
            if debug > 0:
                print("error... host '", hostName, "' not found in dictionary'")
            return("")
            
    def getStatus(self, hostName): 
        debug = 0 
        try:
            h = self.hostDict.get(hostName)
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
            
    def getAPI(self, hostName): 
        debug = 0 
        try:
            h = self.hostDict.get(hostName)
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
            
    def showHostInfo(self, hostName):
        
        print('Host config information for ', hostName, sep="")
        alias = self.getAlias(hostName)
        if alias != "": 
            print("   status:", self.getStatus(hostName))
            print("   alias:", self.getAlias(hostName))
            print("   URL: ", self.getURL(hostName))
            print("   accessKey:", self.getAccessKey(hostName))
            print("   secretKey:", self.getSecretKey(hostName))
            print("   api: ", self.getAPI(hostName))
        else:
            print("-- No host matching '", hostName, "' found is configured.", sep="")
        print()
    
    def main(self):
        searchlist = [
                # 'gcs', 
                #'s3', 
                'c0',
                'c1',
                'c2', 
                'play', 
                'm0', 
                'p1', 
                #'z0', 
                #"local"
                ]
        for h in searchlist:
            self.showHostInfo(h)
        
if __name__ == "__main__" :
    
    getMinioHostInfo().main()
    
    
    
    
