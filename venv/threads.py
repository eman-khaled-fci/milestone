from time import sleep
from threading import *
import pandas as pd
import logging
import random
import queue
import time
import csv
import re
from collections import defaultdict
import timeit



BUF_SIZE = 10
q = queue.Queue (BUF_SIZE)


class ProducerThread (Thread):
    def init(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super (ProducerThread, self).init ()
        self.target = target
        self.name = name

    def run(self):


        if not q.full ():
            chunksize = 10000
            start = timeit.default_timer()
            with pd.read_csv("C:\ofile\\hussinn.csv", chunksize=chunksize, on_bad_lines="skip",
                             encoding="ISO-8859-1",
                             low_memory=False) as reader:                        #O(chunk size)
                for chunk in reader:  #O(chunk size)
                    q.put (chunk)
            stop = timeit.default_timer()
            print("time read",stop - start)

class ConsumerThread (Thread):
    def init(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super (ConsumerThread, self).init ()
        self.target = target
        self.name = name
        return

    def run(self):
        start1=timeit.default_timer()
        badWords = pd.read_csv ("C:\ofile\\full.csv", na_values=" NaN", on_bad_lines="skip", encoding="ISO-8859-1",
                                low_memory=False)                           #O(n)
        regex = re.compile ('|'.join (re.escape (x) for x in badWords["2 girls 1 cup"]),re.IGNORECASE)  #O(n)
        Flag1 = True
        Flag2 = True
        # while q.empty () == False:
        chunk = q.get ()

        for columnname,columncontent in chunk.iterrows():                           #O(chunksize)
            data = str(columncontent[0]) + str(columncontent[2]) + str(columncontent[6])
            data2 = chunk.iloc[:, 0] + chunk.iloc[:, 2] + chunk.iloc[:, 6]
            match1 = re.search(regex, str(columncontent[0]))                  
            match2 = re.search(regex, str(columncontent[2]))
            match3 = re.search(regex, str(columncontent[6]))

            exist = chunk.loc[data2==data]            #O(chunksize)
            if(match1!=None or match2!=None or match3!=None):
                if Flag1:
                    exist.to_csv('C:\ofile\\unhealthy.csv', mode="w", header=False)
                    Flag1 = False
                else:
                    exist.to_csv('C:\ofile\\unhealthy.csv', mode="a", header=False)
            else:
                if Flag2:
                    exist.to_csv('C:\ofile\\healty.csv', mode="w", header=False)
                    Flag2 = False
                else:
                    exist.to_csv('C:\ofile\\healty.csv', mode="a", header=False)

        stop1=timeit.default_timer()
        print("time write",stop1-start1)
#O(chunksize)=O(n)
if name == 'main':
    p = ProducerThread (name='producer')
    c = ConsumerThread (name='consumer')

    p.start ()
    time.sleep (2)
    c.start ()
    time.sleep (2)
    stop = timeit.default_timer()