__author__ = 'matthew'


import numpy as np
from collections import defaultdict


from parse import *

def calc_rel(dwell_time):
    if dwell_time < 50:
        return 0
    elif dwell_time < 400:
        return 1
    else:
        return 2

def get_rel_map(eval_file):
    f = open(eval_file)
    fr = FileReader(f)

    rel_map = defaultdict(dict)

    while True:
        line = fr.get_next_line()
        if not line:
            break
        elems = line.split()
        if elems[2] != 'T':
            continue
        test_query = parse_query(elems)
        session_id = test_query['session_id']
        serp_id = test_query['serp_id']
        while True:
            line = fr.peek_line()
            if not line:
                break
            elems = line.split()
            # done with session
            if elems[1] == 'M':
                break
            line = fr.get_next_line()
            if elems[2] == 'C' and int(elems[3]) == serp_id:
                click = parse_click(elems)
                time_passed = click['time_passed']
                url_id = click['url_id']
                # look ahead to determine dwell time
                next_line = fr.peek_line()
                # if this is the last click of the file
                if not next_line:
                    rel_map[session_id][url_id] = 2
                    break
                next_elems = next_line.split()
                # if this is the last click of the session
                if next_elems[1] == 'M':
                    rel_map[session_id][url_id] = 2
                else:
                    next_time_passed = int(next_elems[1])
                    dwell_time = next_time_passed - time_passed
                    rel = calc_rel(dwell_time)
                    if rel > 0:
                        rel_map[session_id][url_id] = rel

    f.close()
    return rel_map

def compute_NDCG(session_id, url_ranking, rel_map):
    rel_scores = []
    for url_id in url_ranking:
        if url_id in rel_map[session_id]:
            rel_scores.append(rel_map[session_id][url_id])
        else:
            rel_scores.append(0)

    DCG = compute_DCG(rel_scores)
    IDCG = compute_DCG(sorted(rel_scores, reverse=True))
#     if IDCG == 0:
#         print session_id
#         print rel_map[session_id]
#         print rel_scores
#         print 'ZERO'
    return DCG / float(IDCG)

def compute_DCG(rel_scores):
    return rel_scores[0] + sum([ score/np.log2(i+2) for i, score in enumerate(rel_scores[1:])])

def compute_overall_score(ranking_file, rel_map):
    f = open(ranking_file)
    fr = FileReader(f)

    scores = {}

    # skip the first line
    fr.get_next_line()

    while True:
        if not fr.peek_line():
            break

        url_ranking = []
        for _ in range(0,10):
            line = fr.get_next_line()
            session_id, url_id = [int(x) for x in line.rstrip().split(',')]
            url_ranking.append(url_id)

        #print session_id, url_ranking
        score = compute_NDCG(session_id, url_ranking, rel_map)

        scores[session_id] = score

    f.close()

    return np.mean(scores.values())


