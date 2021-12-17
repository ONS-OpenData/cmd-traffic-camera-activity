# cmd-traffic-camera-activity

Script for transforming traffic camera activity.

Transform takes 1 input xlsx file, remove any previous xlsx files so there is no confusion in which spreadsheet is picked up. The input file is found from the ONS website https://www.ons.gov.uk/economy/economicoutputandproductivity/output/datasets/trafficcameraactivity

Place the input file in the same directory as the script.

1 output file is created
- v4-traffic-camera-activity.csv

There is some sparsity within this dataset, which the SparsityFiller function takes care of.

The transform requires the use of databaker, databakerUtils.sparsityFunctions, databakerUtils.v4Functions, databakerUtils.writers & api_pipeline
