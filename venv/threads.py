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


BUF_SIZE = 10
q = queue.Queue (BUF_SIZE)


class ProducerThread (Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super (ProducerThread, self).__init__ ()
        self.target = target
        self.name = name

    def run(self):


        if not q.full ():
            chunksize = 10 ** 5
            with pd.read_csv("C:\ofile\\hussinn.csv", chunksize=chunksize, on_bad_lines="skip", nrows=200,
                             encoding="ISO-8859-1",
                             low_memory=False) as reader:                        #O(chunk size)
                for chunk in reader:  #O(chunk size)
                    q.put (chunk)


class ConsumerThread (Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, verbose=None):
        super (ConsumerThread, self).__init__ ()
        self.target = target
        self.name = name
        return

    def run(self):
        badWords = pd.read_csv ("C:\ofile\\full.csv", na_values=" NaN", on_bad_lines="skip", encoding="ISO-8859-1",
                                low_memory=False)                           #O(n)
        regex = re.compile ('|'.join (re.escape (x) for x in badWords["2 girls 1 cup"]),re.IGNORECASE)  #O(n)
        Flag1 = True
        Flag2 = True
        # while q.empty () == False:
        chunk = q.get ()

        for columnname,columncontent in chunk.iterrows():                           #O(chunksize)
            data = columncontent[0] + columncontent[2] + columncontent[6]
            data2 = chunk.iloc[:, 0] + chunk.iloc[:, 2] + chunk.iloc[:, 6]
            match1 = re.search(regex, columncontent[0])
            match2 = re.search(regex, columncontent[2])
            match3 = re.search(regex, columncontent[6])                    

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
#O(chunksize)=O(n)
    # if __name__ == '__main__':
    p = ProducerThread (name='producer')
    c = ConsumerThread (name='consumer')

    p.start ()
    time.sleep (2)
    c.start ()
    time.sleep (2)