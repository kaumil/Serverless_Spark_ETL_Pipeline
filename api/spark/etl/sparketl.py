
#MISSING VALUE ANALYSIS


import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType,StringType
from pyspark.sql.functions import *
from pyspark.sql.functions import isnan, when, count, col
#ML Algorithms
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LinearSVC

from google.colab import drive
drive.mount('/content/gdrive')
path_prefix = "/content/gdrive/MyDrive/cadorsdata"

import sys
sys.path.insert(1, path_prefix)

import os
totalFiles=0
for base, dirs, files in os.walk('/content/gdrive/MyDrive/cadorsdata'):
    print('Searching in : ',base)
    for Files in files:
        totalFiles += 1
print(totalFiles)

spark = SparkSession.builder.appName('ETL temp').getOrCreate()
# assert spark.version >= '3.0' # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

df = spark.read.json('/content/gdrive/MyDrive/cadorsdata/*')

############ Missing value analysis ################

###1) Occurence Category--> 1139 distinct categories with no NA's
transform_udf=udf(merge,StringType())
temp=df.withColumn('Occurrence Category_merged',transform_udf(col('Occurrence Category')))
print(temp.head(10))
print(temp.filter(col('Occurrence Category_merged')=='NA').count())
print(temp.filter(col('Occurrence Category_merged')=='').count())
temp=temp.select(['Occurrence Category_merged']).distinct()
print(temp.count())
# print(temp.collect())

###2) Occurence Event info--> 144 distinct occurence events combinations with 8021 NA's
transform_udf=udf(merge,StringType())
temp=df.withColumn('Occurrence Event Information_merged',transform_udf(col('Occurrence Event Information')))
print(temp.head(10))
print(temp.filter(col('Occurrence Event Information_merged')=='NA').count())
print(temp.filter(col('Occurrence Event Information_merged')=='').count())
temp=temp.select(['Occurrence Event Information_merged']).distinct()
print(temp.count())
# print(temp.collect())

##3) Month ---- No bad data in date
temp=df.select(['Occurrence Date:'])
print(temp.select([count(when(col(c).contains('None') | \
                            col(c).contains('NULL') | \
                            (col(c) == '' ) | \
                            col(c).isNull() | \
                            isnan(c), c 
                          )).alias(c)
                    for c in temp.columns]).show())
print(temp.head(1))
print(temp.select(col('Occurrence Date:'),month(to_date(col('Occurrence Date:'),"yyyy-MM-dd")).alias('month')).show())

##4) Province ---- 243 bad data , 41 distinct provinces
temp=df.select(['Province:','TC Region:'])
temp=temp.withColumn("GR",when(col('Province:')!='',col('Province:')).otherwise(col('TC Region:')))

print(temp.distinct().count())
# print(temp.collect())
print(temp.select([count(when(col(c).contains('None'),c)).alias(c)
                    for c in temp.columns]).show())
print(temp.select([count(when(col(c).contains('NULL'),c)).alias(c)
                    for c in temp.columns]).show())
print(temp.select([count(when(col(c) == '',c)).alias(c)
                    for c in temp.columns]).show())
print(temp.select([count(when(col(c).isNull(),c)).alias(c)
                    for c in temp.columns]).show())
print(temp.select([count(when(isnan(c),c)).alias(c)
                    for c in temp.columns]).show())

#5) Day or Night ---- Not taking this rather taking hour of incident . Hour
print(temp.select([count(when(col(c) == '',c)).alias(c)
                    for c in temp.columns]).show())

temp=df.select(['Occurrence Time:','Day or Night:'])
temp=temp.withColumn('hour',hour(to_timestamp(substring(col('Occurrence Time:'),1,4),'HHmm')))
temp=temp.withColumn('timestamp',to_timestamp(substring(col('Occurrence Time:'),1,4),'HHmm'))
temp=temp.filter(temp['Day or Night:']=='night-time')
print(temp.groupby('Day or Night:').agg(max('timestamp')).show())
print(temp.groupby('Day or Night:').agg(min('timestamp')).show())

print(temp.show())


##6) Aircraft Features, only 8369 has aircraft involved

featureset2 = aircraft_information1.groupBy('CADORS Number').agg(collect_list('flight_rule').alias('flight_rule'),collect_list('model').alias('model'),collect_list('year_built').alias('year_built'),collect_list('amateur_built').alias('amateur_built'),collect_list('gear_type').alias('gear_type'),collect_list('phase_of_flight').alias('phase_of_flight'),collect_list('damage').alias('damage'),collect_list('operator_type').alias('operator_type'))
# print(featureset2.count())
featureset2=featureset2.withColumn('flight_rule_merged',transform_udf(col('flight_rule'))).withColumn('model_merged',transform_udf(col('model'))).withColumn('year_built_merged',transform_udf(col('year_built'))).withColumn('amateur_built_merged',transform_udf(col('amateur_built'))).withColumn('gear_type_merged',transform_udf(col('gear_type'))).withColumn('phase_of_flight_merged',transform_udf(col('phase_of_flight'))).withColumn('damage_merged',transform_udf(col('damage'))).withColumn('operator_type_merged',transform_udf(col('operator_type')))
# print(featureset2.count())
print(featureset2.head(2))
temp=featureset2.select(['flight_rule_merged','model_merged','year_built_merged','amateur_built_merged','gear_type_merged','phase_of_flight_merged','damage_merged','operator_type_merged'])
print(temp.select([count(when(col(c) == '',c)).alias(c)
                    for c in temp.columns]).show())

temp=temp.select([when(col(c)=="",'NA').otherwise(col(c)).alias(c) for c in temp.columns])
print(temp.head(2))
print(temp.select([count(when(col(c) == '',c)).alias(c)
                    for c in temp.columns]).show())
print(temp.select([count(when(col(c) == 'NA',c)).alias(c)
                    for c in temp.columns]).show())
                   
##7) Weather Features, out of 9000, 6200 if prcp and snow kept , 2500 if wdir is added ################################

temp=weather_df.select(['CADORS Number','prcp','snow'])
# temp=weather_df.select(['CADORS Number','prcp','snow','wdir','wspd','wpgt'])
print(temp.count())
# temp=temp.na.drop()
temp=temp.dropna(how='any')
print(temp.count())
print(temp.select([count(when(col(c).isNull(),c)).alias(c)
                    for c in temp.columns]).show())
print(temp.select([count(when(isnan(c),c)).alias(c)
                    for c in temp.columns]).show())
print(temp.head(2))

"""CREATING AND WRITING INTERMEDIATRY SPARK DATAFRAMES

"""



import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType,StringType
from pyspark.sql.functions import *
from pyspark.sql.functions import isnan, when, count, col
#ML Algorithms
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, VectorAssembler, SQLTransformer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.classification import LinearSVC

from google.colab import drive
drive.mount('/content/gdrive')
path_prefix = "/content/gdrive/MyDrive/cadorsdata"

import sys
sys.path.insert(1, path_prefix)

import os
totalFiles=0
for base, dirs, files in os.walk('/content/gdrive/MyDrive/cadorsdata'):
    print('Searching in : ',base)
    for Files in files:
        totalFiles += 1
print(totalFiles)

spark = SparkSession.builder.appName('ETL temp').getOrCreate()
# assert spark.version >= '3.0' # make sure we have Spark 3.0+
spark.sparkContext.setLogLevel('WARN')
sc = spark.sparkContext

raw_data = spark.read.json('/content/gdrive/MyDrive/cadorsdata/*')
# raw_data = spark.read.json('/content/gdrive/MyDrive/cadorsdata/wx-ffe01746-5818-4e1a-b73d-236ab52fb385.json')

raw_data1=raw_data.dropDuplicates()
raw_data2=raw_data1.filter(raw_data1['CADORS Number']!='null')
raw_data3=raw_data2.filter(raw_data2['latitude'].isNull()=='False')

# raw_data3.select(['CADORS Number']).distinct().count()
# raw_data3.count()
# temp=raw_data3.groupBy(raw_data3['CADORS Number']).agg(count("*").alias('cnt'))
# temp.sort(col('cnt').desc()).show()

print(raw_data3.count())

raw_data3.printSchema()

#Remove if this full data to be considered
# df=raw_data.filter(col('Occurrence Type:')=='Accident').cache()
df=raw_data3.select(['CADORS Number','Occurrence Type:','Occurrence Category','Occurrence Event Information','Occurrence Date:','Country:','Occurrence Time:','Fatalities:','Injuries:','Occurrence Location:','Province:','TC Region:','latitude','longitude','Aircraft Information'])

df = df.withColumn('Injuries:', df['Injuries:'].cast(IntegerType()))
df = df.withColumn('Fatalities:', df['Fatalities:'].cast(IntegerType()))
df = df.withColumn('month',month(to_date(col('Occurrence Date:'),"yyyy-MM-dd")))
df = df.withColumn('year',year(to_date(col('Occurrence Date:'),"yyyy-MM-dd")))
df = df.withColumn("GR",when(col('Province:')!='',col('Province:')).otherwise(col('TC Region:')))
df = df.filter(df['Occurrence Time:']!='')
df = df.withColumn('hour',hour(to_timestamp(substring(col('Occurrence Time:'),1,4),'HHmm')))
df = df.withColumn("label", when(col('Fatalities:')+col('Injuries:')>1,1).otherwise(0))

#Making Aircraft Dataframe
af=df.select(df['CADORS Number'],df['Aircraft Information']).cache()
af=af.withColumn('Flight Rule',af['Aircraft Information']['Flight Rule:']).withColumn('Model',af['Aircraft Information']['Model:']).withColumn('Registration Mark',af['Aircraft Information']['Registration Mark:']).withColumn('Aircraft Category',af['Aircraft Information']['Aircraft Category:']).withColumn('Make',af['Aircraft Information']['Make:']).withColumn('Year Built',af['Aircraft Information']['Year Built:']).withColumn('Amateur Built',af['Aircraft Information']['Amateur Built:']).withColumn('Gear Type',af['Aircraft Information']['Gear Type:']).withColumn('Phase of Flight',af['Aircraft Information']['Phase of Flight:']).withColumn('Damage',af['Aircraft Information']['Damage:']).withColumn('Operator Type',af['Aircraft Information']['Operator Type:'])
ai1=af.select(af['CADORS Number'],posexplode(af['Registration Mark']))
ai2=af.select(af['CADORS Number'],posexplode(af['Model']))
ai5=af.select(af['CADORS Number'],posexplode(af['Flight Rule']))
ai6=af.select(af['CADORS Number'],posexplode(af['Aircraft Category']))
ai8=af.select(af['CADORS Number'],posexplode(af['Make']))
ai10=af.select(af['CADORS Number'],posexplode(af['Year Built']))
ai11=af.select(af['CADORS Number'],posexplode(af['Amateur Built']))
ai15=af.select(af['CADORS Number'],posexplode(af['Gear Type']))
ai16=af.select(af['CADORS Number'],posexplode(af['Phase of Flight']))
ai17=af.select(af['CADORS Number'],posexplode(af['Damage']))
ai19=af.select(af['CADORS Number'],posexplode(af['Operator Type']))
aircraft_information = ai1.join(ai2, ['pos','CADORS Number']).join(ai5, ['pos','CADORS Number']).join(ai6, ['pos','CADORS Number']).join(ai8, ['pos','CADORS Number']).join(ai10, ['pos','CADORS Number']).join(ai11, ['pos','CADORS Number']).join(ai15, ['pos','CADORS Number']).join(ai16, ['pos','CADORS Number']).join(ai17, ['pos','CADORS Number']).join(ai19, ['pos','CADORS Number'])
# print(aircraft_information.show())
aircraft_information1 = aircraft_information.toDF('pos','CADORS Number','registration_mark','model','flight_rule','aircraft_category','make','year_built','amateur_built','gear_type','phase_of_flight','damage','operator_type')
# print(aircraft_information1.show())
aircraft_df=df.select(['CADORS Number','Country:','Occurrence Type:','Occurrence Category','Occurrence Event Information','Occurrence Date:','Fatalities:','Injuries:','Occurrence Location:','Province:','GR','hour','month','label','latitude','longitude']).join(aircraft_information1,['CADORS Number'],how='left')
print(aircraft_df.show(5))

# print(aircraft_df.count())
# print(aircraft_df.dropDuplicates().count())
aircraft_df.write.json("/content/gdrive/MyDrive/cadorsdata_aircraft_V2/")

aircraft_df = aircraft_df.withColumn('year',year(to_date(col('Occurrence Date:'),"yyyy-MM-dd")))
# aircraft_df.dtypes
aircraft_df=aircraft_df.na.fill(value=0,subset=["Fatalities:",'Injuries:'])
aircraft_df=aircraft_df.replace('','NA')

aircraft_df.write.json("/content/gdrive/MyDrive/cadorsdata_aircraft_V3/")

#Arshdeep Code

data1=aircraft_df.select(aircraft_df['CADORS Number'],
                         aircraft_df['Country:'],
                         aircraft_df['aircraft_category'],
                         aircraft_df['make'],
                         aircraft_df['year_built'],
                         aircraft_df['phase_of_flight'],
                         aircraft_df['operator_type'],
                         aircraft_df['Fatalities:'],
                         aircraft_df['Injuries:'],
                         aircraft_df['Occurrence Category'],
                         aircraft_df['Occurrence Type:'],
                         aircraft_df['month'],
                         aircraft_df['Province:'],
                         aircraft_df['latitude'],
                         aircraft_df['longitude'],
                         aircraft_df['year'],
                         )
# data1.show()
data1.write.json("/content/gdrive/MyDrive/cadorsdata_aircraft_arshdeep_V1/")

############ Weather Data #############
wdf=raw_data3.select(raw_data3['CADORS Number'],raw_data3['weather'])
wdf1=wdf.join(df.select(['CADORS Number']),['CADORS Number'])
print(wdf1.count())
print(wdf1.select(['CADORS Number']).distinct().count())
weather_df=wdf1.select('CADORS Number', 'weather.*')

weather_df.show()