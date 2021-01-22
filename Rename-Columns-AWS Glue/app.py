# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import json
import boto3
import logging
import os
from urllib.parse import unquote_plus

logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

glue = boto3.client('glue')
GlueServiceRole = os.environ['GlueServiceRole']
DestinationBucket = os.environ['DestinationBukcet']


def lambda_handler(event, context):
  logger.info(json.dumps(event))
  for record in event['Records']:
#Grab the file name from the event record which triggered the lambda function & Construct the path for data file, name file and renamed file. 
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        data_file_name = key.split('/')[0]
        s3_data_file_path = 's3://' + bucket + '/' + data_file_name +'/'
        name_file_bucket = DestinationBucket
        name_file_prefix = data_file_name+'/'+ data_file_name + 'NameFile'
        name_file_path = 's3://'+name_file_bucket+'/' + name_file_prefix 
      
#Create crawler for the data file if it does not already exist and run it.   
        try:
            crawler = glue.get_crawler(Name=data_file_name)
        except glue.exceptions.EntityNotFoundException as e:
            crawler = glue.create_crawler(
                Name=data_file_name,
                    Role= GlueServiceRole,
                    DatabaseName='sampledb',
                    Description='Crawler for data files',
                    Targets={
                        'S3Targets': [
                            {
                                'Path': s3_data_file_path,
                                'Exclusions': [
                                ]
                            },
                        ]
                    },
                    SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DELETE_FROM_DATABASE'
                }
                #,Configuration='{ "Version": 2.0, "CrawlerOutput": { "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" } } }'
            )
            response = glue.start_crawler(
            Name=data_file_name)
        else:
            response = glue.start_crawler(
            Name=data_file_name)
            
#Create crawler for the name file if it does not already exist and run it.   

        try:
            crawler = glue.get_crawler(Name=data_file_name + '_name_file')
        except glue.exceptions.EntityNotFoundException as e:
            crawler = glue.create_crawler(
                    Name=data_file_name + '_name_file',
                    Role= GlueServiceRole,
                    DatabaseName='sampledb',
                    Description='Crawler for name files',
                    Targets={
                        'S3Targets': [
                            {
                                'Path': name_file_path,
                                'Exclusions': [
                                ]
                            },
                        ]
                    },
                    SchemaChangePolicy={
                    'UpdateBehavior': 'UPDATE_IN_DATABASE',
                    'DeleteBehavior': 'DELETE_FROM_DATABASE'
                    }
                #,Configuration='{ "Version": 2.0, "CrawlerOutput": { "Partitions": { "AddOrUpdateBehavior": "InheritFromTable" } } }'
                    )
            response = glue.start_crawler(Name=data_file_name+'_name_file')
        else:
            response = glue.start_crawler(Name=data_file_name+'_name_file')

#Run the agnostic Glue job to renamed the files by passing the file name argument. 

        try:
            glue.start_job_run( Arguments = {'--FileName': data_file_name, '--DestinationBucketName' : DestinationBucket })
        except Exception as e:
            print('Glue Job runtime Issue')
