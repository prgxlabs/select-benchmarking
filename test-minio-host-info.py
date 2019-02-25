# -*- coding: utf-8 -*-
"""
Created on Thu Feb 21 10:10:58 2019

@author: smccle01
"""

#!/usr/bin/env/env python3

import miniohostinfo as mc

def showHostInfo( hostName):
        
        print('Host config information for ', hostName, sep="")
        alias = mc.getMinioHostInfo().getAlias(hostName)
        if alias != "": 
            print("   status:", mc.getMinioHostInfo().getStatus(hostName))
            print("   alias:", mc.getMinioHostInfo().getAlias(hostName))
            print("   URL: ", mc.getMinioHostInfo().getURL(hostName))
            print("   accessKey:", mc.getMinioHostInfo().getAccessKey(hostName))
            print("   secretKey:", mc.getMinioHostInfo().getSecretKey(hostName))
            print("   api: ", mc.getMinioHostInfo().getAPI(hostName))
        else:
            print("-- No host matching '", hostName, "' found is configured.", sep="")
        print()
#main    
if __name__ == "__main__" :
    
    
    mc.getMinioHostInfo()
    
    showHostInfo('c0')
    showHostInfo('c1')
    #showHostInfo('c2')
    #showHostInfo('c3')
    
    #showHostInfo('crap')