from os.path import join
from glob import glob
from collections import namedtuple
import scripts.package.sql_meta as sqm
import scripts.package.sql_load as cq
import rtyaml
import os
# load up the schemas
# explicitly map them to file names
SCHEMAS_DIR = './meta/schemas'
SCHEMA_PATHS = {
    'committee_memberships': 'congress_legislators/committee_memberships',
    'committees': "congress_legislators/committees",
    'fec_ids': "congress_legislators/fec_ids",
    'legislators': "congress_legislators/legislators",
    'social_media_accounts': "congress_legislators/social_media_accounts",
    'terms': "congress_legislators/terms",
    # examples of mapping a slug to a different filename
    'fec_candidate_summaries': 'fec/candidate_summaries',
    'friendships': "twitter/friendships",
    'tweets': "twitter/tweets",
    'twitter_profiles': "twitter/twitter_profiles"
}
## load up the schemas
xs = {}
for slug, bn in SCHEMA_PATHS.items():
    fn = join(SCHEMAS_DIR, bn + '.yaml')
    xs[slug] = rtyaml.load(open(fn))

MyTable = namedtuple('MyTable', "table_name, schema")

## Tables I want
mytables = [
    MyTable('committee_memberships', xs['committee_memberships']),
    MyTable('committees', xs['committees']),
    MyTable('fec_ids', xs['fec_ids']),
    MyTable('legislators', xs['legislators']),
    MyTable('social_media_accounts', xs['social_media_accounts']),
    MyTable('terms', xs['terms']),
    MyTable('fec_candidate_summaries', xs['fec_candidate_summaries']),
    MyTable('tweets', xs['tweets']),
    MyTable('congress_twitter_profiles', xs['twitter_profiles'])
]

TMPNAME = "/tmp/funzzzz.sqlite"
try:
    os.remove(TMPNAME)
except OSError:
    pass
mdb = sqm.metadbize(mytables, TMPNAME )
# print(sqm.meta_to_sql(mdb))
mdb.create_all()

# Now load the data
print("Loading data")
data = open("./stash/seeds/twitter/congress-profiles.csv")
cq.load_data_table(mdb, 'congress_twitter_profiles', data)


data = open("./stash/seeds/congress_legislators/fec_ids.csv")
cq.load_data_table(mdb, 'fec_ids', data)
data = open("./stash/seeds/congress_legislators/legislators.csv")
cq.load_data_table(mdb, 'legislators', data)
data = open("./stash/seeds/congress_legislators/committee-memberships.csv")
cq.load_data_table(mdb, 'committee_memberships', data)
data = open("./stash/seeds/congress_legislators/committees.csv")
cq.load_data_table(mdb, 'committees', data)
data = open("./stash/seeds/congress_legislators/social-media-accounts.csv")
cq.load_data_table(mdb, 'social_media_accounts', data)
data = open("./stash/seeds/congress_legislators/terms.csv")
cq.load_data_table(mdb, 'terms', data)

print("Loading tweets...")
# for idx, fn in enumerate(glob("./stash/seeds/twitter/tweets/*.csv")):
for idx, fn in enumerate(glob("./stash/seeds/twitter/tweets/*.csv")):
    print(idx, fn)
    cq.load_data_table(mdb, 'tweets', open(fn))


