__author__ = 'matthew'

import numpy as np

from mrjob.job import MRJob
from itertools import combinations, permutations
from math import sqrt

from scipy.stats.stats import pearsonr

class CountClicks(MRJob):

    def steps(self):
        thesteps = [
            self.mr(mapper=self.line_mapper, combiner=self.counter_local, reducer=self.counter),
        ]
        return thesteps

    # SessionID TimePassed TypeOfRecord SERPID URLID
    def line_mapper(self,_,line):
        elems = line.split()
        if elems[2] == 'C':
            yield (int(elems[4]), 1)

    def counter_local(self, key, values):
        yield (key, sum(values))

    def counter(self, key, values):
        yield (key, sum(values))

#Below MUST be there for things to work!
if __name__ == '__main__':
    CountClicks.run()


# ~/anaconda/bin/python computesim.py -r emr --num-ec2-instances 5 subset-full.csv > output.full.emr.txt