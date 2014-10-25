__author__ = 'matthew'

import numpy as np
import pandas as pd

# define helper classes/functions
class FileReader:
    def __init__(self, f):
        self.f = f
        self.next_line = self.f.readline()
    def peek_line(self):
        return self.next_line
    def get_next_line(self):
        temp = self.next_line
        self.next_line = self.f.readline()
        return temp

def parse_metadata(elems):
    metadata = {}
    metadata['session_id'] = int(elems[0])
    metadata['type_of_record'] = elems[1]
    metadata['Day'] = int(elems[2])
    metadata['user_id'] = int(elems[3])
    return metadata

def parse_query(elems):
    query = {}
    query['session_id'] = int(elems[0])
    query['time_passed'] = int(elems[1])
    query['type_of_record'] = elems[2]
    query['serp_id'] = int(elems[3])
    query['query_id'] = int(elems[4])
    query['list_of_terms'] = [int(term) for term in elems[5].split(',')]
    query['list_of_urls_and_domains'] = [(int(url), int(domain)) for url,domain in [tuple(term.split(',')) for term in elems[6:]]]
    return query

def parse_click(elems):
    click = {}
    click['session_id'] = int(elems[0])
    click['time_passed'] = int(elems[1])
    click['type_of_record'] = elems[2]
    click['serp_id'] = int(elems[3])
    click['url_id'] = int(elems[4])
    return click

def get_sessions(fname, num_sessions=np.inf):
    sessions = []
    tests = []
    queries = []
    clicks = []
    metas = {}

    f = open(fname)
    filereader = FileReader(f)

    while True:
        if len(sessions) > num_sessions:
            break
        line = filereader.get_next_line()
        elems = line.split()
        if not elems:
            break
        metadata = parse_metadata(elems)
        sessions.append(metadata)
        metas[metadata['session_id']] = metadata['user_id']
        while True:
            elems = filereader.peek_line().split()
            if not elems:
                break
            if elems[1] == 'M':
                break
            else:
                elems = filereader.get_next_line().split()
                if elems[2] == 'Q':
                    query = parse_query(elems)
                    query['user_id'] = metas[query['session_id']]
                    queries.append(query)
                if elems[2] == 'T':
                    test = parse_query(elems)
                    test['user_id'] = metas[test['session_id']]
                    tests.append(test)
                if elems[2] == 'C':
                    click = parse_click(elems)
                    click['user_id'] = metas[click['session_id']]
                    clicks.append(click)

    f.close()

    return sessions, queries, tests, clicks, metas


def get_all_sessions_for_user(fname, user_id):
    sessions = []
    tests = []
    queries = []
    clicks = []

    f = open(fname)
    filereader = FileReader(f)

    lines = 0

    while True:
        lines += 1
        if lines % 1e7 == 0:
            print 'Processed %d lines, found %d sessions.' % (lines, len(sessions))
        elems = filereader.get_next_line().split()
        if not elems:
            break
        if elems[1] == 'M' and int(elems[3]) == user_id:
            metadata = parse_metadata(elems)
            sessions.append(metadata)
            while True:
                elems = filereader.peek_line().split()
                if elems[1] == 'M':
                    break
                else:
                    elems = filereader.get_next_line().split()
                    if elems[2] == 'Q':
                        query = parse_query(elems)
                        queries.append(query)
                    if elems[2] == 'T':
                        test = parse_query(elems)
                        tests.append(test)
                    if elems[2] == 'C':
                        click = parse_click(elems)
                        clicks.append(click)

    return sessions, queries, tests, clicks