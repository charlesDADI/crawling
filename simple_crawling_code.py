# -*- coding: utf-8 -*-
"""
Created on Mon Feb 17 13:56:10 2014

@author: root
"""

import urllib2
opener = urllib2.build_opener()
from bs4 import BeautifulSoup

from BeautifulSoup import *
from urlparse import urljoin
import re
import pandas as pd
import datetime
import time


#Define a list for all site to be crawled
pagelist=['http://www.charlesdadi.fr.nf/', 'http://en.wikipedia.org/wiki/Machine_learning']

def main():
    #Define a list structure to keep metadata for each website
    websitedata=list()
    i = 0;
    for page in pagelist:
        newpages=[]
        try:
            c=urllib2.urlopen(page)
        except:
            print "%s could not be  opened!" % page
            continue
        soup=BeautifulSoup(c.read( ))

        #get all balise stating by 'a'
        soup.findAll('a')

        links=soup.findAll('a')
        #print every link on the current page
        url=[]
        for link in links:
                if ('href' in dict(link.attrs)):
                  url.append(urljoin(page,link['href']))
        for u in url:
            if  re.finditer(r'(https?://\S+)',str(u)) and  u not in newpages:
               newpages.append(str(u))

        websitedata.append(dict({"Parent_Url":page,"Title":soup.title.string,"Text":soup.text,"Url_Tree":newpages,"Datereview":datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}))
        i +=1



def export2mongoDB():
    import sys
    from pymongo import Connection
    from pymongo.errors import ConnectionFailure
    """ Connect to MongoDB """
    try:
        c = Connection(host="localhost", port=27017)
        print "Connected successfully"
    except ConnectionFailure, e:
        sys.stderr.write("Could not connect to MongoDB: %s" % e)
        sys.exit(1)
    dbh = c["crawling"]
    [dbh.machine_learning.insert(t, safe=True) for t in websitedata]


if __name__ == "__main__":
    main()
    export2mongoDB()