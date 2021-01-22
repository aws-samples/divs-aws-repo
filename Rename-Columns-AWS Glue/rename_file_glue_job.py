# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job


args = getResolvedOptions(sys.argv, ['DestinationBucketName', 'FileName']) 

glueContext = GlueContext(SparkContext.getOrCreate())
job = Job(glueContext)


column_name_dynamicframe = glueContext.create_dynamic_frame.from_catalog(
       database = "sampledb",
       table_name = args['FileName']+'namefile')

userdata_dynamicframe = glueContext.create_dynamic_frame.from_catalog(
       database = "sampledb",
       table_name = args['FileName'])

column_name_df = column_name_dynamicframe.toDF()
data_name_df = userdata_dynamicframe.toDF()

column_names_array = column_name_df.columns
data_names_array = data_name_df.columns

command =""
length = len(column_names_array)
for x in range(length):
    command += '("'+data_names_array[x]+'" , "' +column_names_array[x]+'"),'
command = command[:-1]
command = "applymapping1 = ApplyMapping.apply(frame = userdata_dynamicframe, mappings = [" + command + '], transformation_ctx = "applymapping1")'

exec(command)
print(command)
datasink4 = glueContext.write_dynamic_frame.from_options(frame = applymapping1, connection_type = "s3", connection_options = {"path": "s3://"+args['DestinationBucketName']+"/"+args['FileName']+'/'+args['FileName']+'FileRenamed'}, format = "orc", transformation_ctx = "datasink4")


## @type: DataSource
## @args: [database = "sampledb", table_name = "orc_userdataorc", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
##datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "sampledb", table_name = args['FileName'], transformation_ctx = "datasource0")
## @type: ApplyMapping
## @args: [mapping = [("_col0", "timestamp", "Date", "timestamp"), ("_col1", "int", "ID", "int"), ("_col2", "string", "FirstName", "string"), ("_col3", "string", "LastName", "string"), ("_col4", "string", "Email", "string"), ("_col5", "string", "Gender", "string"), ("_col6", "string", "IP", "string"), ("_col7", "string", "Mac", "string"), ("_col8", "string", "Country", "string"), ("_col9", "string", "DOB", "string"), ("_col10", "double", "Duration", "double"), ("_col11", "string", "Title", "string"), ("_col12", "string", "Feedback", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
##applymapping1 = ApplyMapping.apply(frame = datasource0, mappings = [("_col0", "timestamp", "Date", "timestamp"), ("_col1", "int", "ID", "int"), ("_col2", "string", "FirstName", "string"), ("_col3", "string", "LastName", "string"), ("_col4", "string", "Email", "string"), ("_col5", "string", "Gender", "string"), ("_col6", "string", "IP", "string"), ("_col7", "string", "Mac", "string"), ("_col8", "string", "Country", "string"), ("_col9", "string", "DOB", "string"), ("_col10", "double", "Duration", "double"), ("_col11", "string", "Title", "string"), ("_col12", "string", "Feedback", "string")], transformation_ctx = "applymapping1")
## @type: ResolveChoice
## @args: [choice = "make_struct", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2