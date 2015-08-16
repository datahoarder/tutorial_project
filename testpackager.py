from os.path import join
from glob import glob
from collections import namedtuple
from scripts.package.databaser import Databaser
import rtyaml
import os
import csv

# load up the schemas
# explicitly map them to file names
METAS_DIR = './meta/tables'
SEEDS_DIR = './stash/seeds/'
METAS = {}
METAS['congress_twitter_profiles'] = ('twitter/twitter_profiles',
    'twitter/congress-profiles.csv')
METAS['committee_memberships'] = ('congress_legislators/committee_memberships',
    'congress_legislators/committee-memberships.csv')
METAS['fec_ids'] = ('congress_legislators/fec_ids',
    'congress_legislators/fec_ids.csv')
METAS['legislators'] = ('congress_legislators/legislators',
    'congress_legislators/legislators.csv')
METAS['committees'] = ('congress_legislators/committees',
    'congress_legislators/committees.csv')
# METAS['friendships'] = ('twitter/friendships', 'twitter/top-friendships.csv')
# METAS['fec_candidate_summaries'] = ('fec/candidate_summaries',
#     'congress_legislators/committees.csv')

RegTable = namedtuple('RegTable', "table_name, table_meta, data")
regtables = {}
for tname, v in METAS.items():
    meta = rtyaml.load(open(join(METAS_DIR, v[0] + '.yaml')))
    data = csv.DictReader(open(join(SEEDS_DIR, v[1])))
    regtables[tname] = RegTable(tname, meta, data)

TMPNAME = "/tmp/funzzzz.sqlite"
try:
    os.remove(TMPNAME)
except OSError:
    pass

db = Databaser(TMPNAME)
for rt in regtables.values():
    table_name, table_meta, data = rt
    print("Loading", table_name)
    db.bootstrap_table(table_name, table_meta, data)


# # Now load the data
# print("Loading data")

# print("Loading tweets...")
# # for idx, fn in enumerate(glob("./stash/seeds/twitter/tweets/*.csv")):
# for idx, fn in enumerate(glob("./stash/seeds/twitter/tweets/*.csv")):
#     print(idx, fn)
#     cq.load_data_table(mdb, 'tweets', open(fn))
