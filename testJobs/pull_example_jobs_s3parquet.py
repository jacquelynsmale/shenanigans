import pandas
import numpy as np
import json
import hyp3_sdk

# get parquet files from s3://enterprise-campaigns/its-live/campaign/

# local parquet path
parquet_path = '/Users/jrsmale/projects/findTestJobs/s1.parquet'
par = pandas.read_parquet(parquet_path)

scenes_to_test = []
#For s1, s2 or just general batch from parquet
for i in np.linspace(1, 40000, 200):
    reference = par._get_value(i, 'reference')
    secondary = par._get_value(i, 'secondary')
    scenes_to_test.append([reference, secondary])

#If looking for something specific
#scenes_to_test = []
#i = 0
#while len(scenes_to_test) < 200:
#    i += 1
#    if par._get_value(i, 'reference').startswith('LC08') and par._get_value(i, 'secondary').startswith('LC08'):
#        reference = par._get_value(i, 'reference')
#        secondary = par._get_value(i, 'secondary')
#        scenes_to_test.append([reference, secondary])

test_str = json.dumps(scenes_to_test)

with open("autorift_s1_pairs_test.json", "w") as outfile:
    outfile.write(test_str)
