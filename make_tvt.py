# -*- coding: utf-8 -*-
# <nbformat>3.0</nbformat>

# <codecell>

# do all necessary imports
import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import random

from collections import defaultdict
from collections import Counter
import operator
from utils import *

def make_tvt(num_users):

    sessions_train = set()
    sessions_validate = set()
    sessions_test = set()

    users_train = set()
    users_validate = set()
    users_test = set()

    session_to_user_id_map = {}
    user_id_to_sessions = defaultdict(set)

    f = open('train')
    for line in f:
        elems = line.split()
        if elems[1] == 'M':
            session_id = int(elems[0])
            user_id = int(elems[3])
            session_to_user_id_map[session_id] = user_id
            user_id_to_sessions[user_id].add(session_id)
            if user_id > num_users:
                max_session_number = session_id - 1
                break
            day = int(elems[2])
            if day < 21:
                sessions_train.add(session_id)
                users_train.add(user_id)
            elif day < 24:
                sessions_validate.add(session_id)
                users_validate.add(user_id)
            else:
                sessions_test.add(session_id)
                users_test.add(user_id)

    # remove all sessions in sessions_validate with no user context in the train set
    d = users_validate.difference(users_train)
    for user_id in d:
        sessions_validate = sessions_validate.difference(user_id_to_sessions[user_id])

    train_file = open('train-%d' % num_users, 'w')
    validate_file = open('validate', 'w')
    test_file = open('test', 'w')

    f = open('train')
    for line in f:
        elems = line.split()
        session_id = int(elems[0])
        if session_id > max_session_number:
            break
        if session_id in sessions_train:
            train_file.write(line)
        elif session_id in sessions_validate:
            validate_file.write(line)
        else:
            test_file.write(line)

    train_file.close()
    validate_file.close()
    test_file.close()


    process_query_file('validate', num_users)
    process_query_file('test', num_users)

    os.remove('validate')
    os.remove('test')
    os.remove('temp2')

def get_valid_queries(query_file):
    sessions, queries, tests, clicks = get_sessions(query_file)

    sessions_df = pd.DataFrame(sessions)
    queries_df = pd.DataFrame(queries)
    clicks_df = pd.DataFrame(clicks)

    session_ids = sessions_df['session_id'].unique()

    # find all valid queries
    valid_queries_map = defaultdict(list)
    for session_id in session_ids:
        user_id = int(sessions_df[sessions_df['session_id'] == session_id]['user_id'])
        q = queries_df[queries_df['session_id'] == session_id]
        #serp_to_query_id = pd.Series(q['query_id'].values, q['serp_id'].values).to_dict()
        c = clicks_df[clicks_df['session_id'] == session_id]
        action_times = pd.concat( [q[['query_id', 'time_passed', 'type_of_record', 'serp_id']] , c[['time_passed', 'type_of_record', 'serp_id']]] )
        action_times.sort('time_passed', ascending=True, inplace=True)
        time_until_next_action = action_times['time_passed'].diff().shift(-1)
        action_times['time_until_next_action'] = time_until_next_action.fillna(np.inf)
        valid_serps = action_times[(action_times['type_of_record'] == 'C') & (action_times['time_until_next_action'] >= 50)]['serp_id']
        valid_queries = [(session_id, serp_id) for serp_id in valid_serps.values]
        if valid_queries:
            valid_queries_map[user_id].extend(valid_queries)

    return valid_queries_map

def process_query_file(query_file, num_users):

    valid_queries_map = get_valid_queries(query_file)

    # determine randomly the queries to be test queries
    sampled_queries_map = {}
    for user_id in valid_queries_map:
        queries = valid_queries_map[user_id]
        sampled_queries_map[user_id] = random.choice(queries)

    temp = open(query_file)
    temp2 = open('temp2', 'w')

    for line in temp:
        line_to_write = line
        elems = line.split()
        if elems[2] == 'Q':
            session_id = int(elems[0])
            serp_id = int(elems[3])
            #query_id = int(elems[4])
            if (session_id, serp_id) in sampled_queries_map.values():
                elems[2] = 'T'
                line_to_write = '\t'.join(elems) + '\n'
        temp2.write(line_to_write)


    temp.close()
    temp2.close()

    sessions, queries, tests, clicks = get_sessions('temp2')
    tests_df = pd.DataFrame(tests)

    #sessions_to_sample = set(tests_df[tests_df['query_id'].isin(sampled_queries_map.values())]['session_id'].values)
    #print zip(*sampled_queries_map.values())
    sessions_to_sample = zip(*sampled_queries_map.values())[0]
    #print sessions_to_sample

    temp2 = open('temp2')
    final_file = open(query_file + '-%d' % num_users, 'w')

    for line in temp2:
        elems = line.split()
        if int(elems[0]) in sessions_to_sample:
            final_file.write(line)

    temp2.close()
    final_file.close()




def get_default_ranking(file):

    num_users = int(file.split('-')[1])

    sessions, queries, tests, clicks = get_sessions(file)
    tests_df = pd.DataFrame(tests)

    sample_ranking = open('default-ranking-%d' % num_users, 'w')
    sample_ranking.write('SessionID,URLID' + '\n')
    for test in tests:
        session_id = test['session_id']
        list_of_urls = zip(*test['list_of_urls_and_domains'])[0]
        text = [','.join([str(session_id), str(url_id)]) for url_id in list_of_urls]
        for line in text:
            sample_ranking.write(line + '\n')
    sample_ranking.close()

num_users = 100000
make_tvt(num_users)