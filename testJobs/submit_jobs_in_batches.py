from tqdm import tqdm

import hyp3_sdk as sdk
import json


def chunk_list(my_list, chunk_size):
    for ii in range(0, len(my_list), chunk_size):
        yield my_list[ii:ii + chunk_size]


with open('/Users/jrsmale/projects/findTestJobs/autorift_s1_pairs_test.json', 'r') as f:
    scenes_to_test = json.load(f)

print(f'Submitting {len(scenes_to_test)} jobs')
hyp3 = sdk.HyP3('https://hyp3-test-api.asf.alaska.edu')
submitted = 0
for chunk in chunk_list(scenes_to_test, 200):
    jobs = [
        {
            'job_type': 'AUTORIFT',
            'job_parameters': {
                'granules': [
                    scenes[0],
                    scenes[1]
                ],
            },
            'name': 'TestEC2UpdateS1',
        } for scenes in chunk
    ]
    hyp3.submit_prepared_jobs(jobs)
    submitted += len(jobs)
    print(f'{submitted}/{len(scenes_to_test)}')
