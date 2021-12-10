import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession, functions, types
from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import *
from pyspark.sql.functions import isnan, when, count, col
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


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
## @type: DataSource
## @args: [database = "cadorsdw", table_name = "rawdata", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(
    database="cadorsdw", table_name="rawdata", transformation_ctx="datasource0"
)


aircraft_df = spark.read.json("s3://cadorscmpt732/processed_data/*")
weather_df = spark.read.json("s3://cadorscmpt732/processed_data/weather/*")
print(aircraft_df.count())
print(aircraft_df.dropDuplicates())
print(weather_df.count())
print(weather_df.dropDuplicates())

aircraft_df_acci = aircraft_df.filter(col("Occurrence Type:") == "Accident").cache()
weather_df_acci = (
    weather_df.join(aircraft_df_acci.select(["CADORS Number"]), ["CADORS Number"])
    .dropDuplicates()
    .cache()
)
print(aircraft_df_acci.count())
print(aircraft_df_acci.dropDuplicates())
print(weather_df_acci.count())
print(weather_df_acci.dropDuplicates())

weather_df = weather_df.withColumn("prcp", weather_df["prcp"].cast(IntegerType()))
weather_df = weather_df.withColumn("snow", weather_df["snow"].cast(IntegerType()))
weather_df = weather_df.withColumn("pres", weather_df["pres"].cast(IntegerType()))
weather_df = weather_df.withColumn("tavg", weather_df["tavg"].cast(IntegerType()))
weather_df = weather_df.withColumn("tmax", weather_df["tmax"].cast(IntegerType()))
weather_df = weather_df.withColumn("tmin", weather_df["tmin"].cast(IntegerType()))
weather_df = weather_df.withColumn("tsun", weather_df["tsun"].cast(IntegerType()))
weather_df = weather_df.withColumn("wdir", weather_df["wdir"].cast(IntegerType()))
weather_df = weather_df.withColumn("wpgt", weather_df["wpgt"].cast(IntegerType()))
weather_df = weather_df.withColumn("wspd", weather_df["wspd"].cast(IntegerType()))

"""CLASSIFICATION FOR ONLY ACCIDENTS"""


def merge(x):
    if len(x) == 0:
        return "NA"
    temp = ""
    x = sorted(x)
    for i in range(len(x)):
        temp += x[i]
    return temp


def con(x):
    if x == "":
        return "NA"
    return x


transform_udf = udf(merge, StringType())
makeNA_udf = udf(con, StringType())

# picking features from df( Main Data)
featureset1 = aircraft_df_acci.select(
    [
        "CADORS Number",
        "Occurrence Category",
        "hour",
        "GR",
        "Occurrence Event Information",
        "month",
    ]
).dropDuplicates()
featureset1 = featureset1.withColumn(
    "Occurrence Event Information_merged",
    transform_udf(col("Occurrence Event Information")),
).withColumn("Occurrence Category_merged", transform_udf(col("Occurrence Category")))
featureset1 = featureset1.withColumn(
    "Occurrence Event Information_merged",
    makeNA_udf(col("Occurrence Event Information_merged")),
).withColumn(
    "Occurrence Category_merged", makeNA_udf(col("Occurrence Category_merged"))
)
print("FEATURES 1\n")
print(featureset1.count())
print(featureset1.head(5))

# features from aircraft data
# aircraft_information1=aircraft_information1.join(df.select('CADORS Number'),['CADORS Number'])
featureset2 = aircraft_df_acci.groupBy("CADORS Number").agg(
    collect_list("flight_rule").alias("flight_rule"),
    collect_list("year_built").alias("year_built"),
    collect_list("amateur_built").alias("amateur_built"),
    collect_list("gear_type").alias("gear_type"),
    collect_list("phase_of_flight").alias("phase_of_flight"),
    collect_list("damage").alias("damage"),
    collect_list("operator_type").alias("operator_type"),
)
featureset2 = (
    featureset2.withColumn("flight_rule_merged", transform_udf(col("flight_rule")))
    .withColumn("year_built_merged", transform_udf(col("year_built")))
    .withColumn("amateur_built_merged", transform_udf(col("amateur_built")))
    .withColumn("gear_type_merged", transform_udf(col("gear_type")))
    .withColumn("phase_of_flight_merged", transform_udf(col("phase_of_flight")))
    .withColumn("damage_merged", transform_udf(col("damage")))
    .withColumn("operator_type_merged", transform_udf(col("operator_type")))
)
featureset2 = (
    featureset2.withColumn("flight_rule_merged", makeNA_udf(col("flight_rule_merged")))
    .withColumn("year_built_merged", makeNA_udf(col("year_built_merged")))
    .withColumn("amateur_built_merged", makeNA_udf(col("amateur_built_merged")))
    .withColumn("gear_type_merged", makeNA_udf(col("gear_type_merged")))
    .withColumn("phase_of_flight_merged", makeNA_udf(col("phase_of_flight_merged")))
    .withColumn("damage_merged", makeNA_udf(col("damage_merged")))
    .withColumn("operator_type_merged", makeNA_udf(col("operator_type_merged")))
)
print("FEATURES 2\n")
print(featureset2.count())
print(featureset2.head(5))

# #features from weather data
featureset3 = weather_df_acci.select(["CADORS Number", "prcp", "snow"]).dropDuplicates()
featureset3 = featureset3.dropna(how="any")
print("FEATURES 3\n")
print(featureset3.count())
print(featureset3.head(5))

## Final data for ML
ff = featureset1.join(featureset2, ["CADORS Number"], how="inner")
ff = ff.join(featureset3, ["CADORS Number"], how="inner")
print(ff.count())
print(ff.show())
ff.printSchema()

######################### ML DATA ####################################
finalMLData = ff.join(aircraft_df.select(["CADORS Number", "label"]), ["CADORS Number"])
print(finalMLData.filter(finalMLData["label"] == 1).count())
# 771
train, validation = finalMLData.randomSplit([0.75, 0.25])
train = train.cache()
validation = validation.cache()

"""LOGISTIC REGRESSION"""

# ################################# Pipeline ############################################
str_indexer = StringIndexer(
    inputCols=[
        "Occurrence Category_merged",
        "GR",
        "Occurrence Event Information_merged",
        "flight_rule_merged",
        "year_built_merged",
        "amateur_built_merged",
        "gear_type_merged",
        "phase_of_flight_merged",
        "damage_merged",
        "operator_type_merged",
    ],
    outputCols=[
        "Occurrence Category_merged1",
        "GR1",
        "Occurrence Event Information_merged1",
        "flight_rule_merged1",
        "year_built_merged1",
        "amateur_built_merged1",
        "gear_type_merged1",
        "phase_of_flight_merged1",
        "damage_merged1",
        "operator_type_merged1",
    ],
    handleInvalid="keep",
)
assembler = VectorAssembler(
    inputCols=[
        "hour",
        "month",
        "Occurrence Category_merged1",
        "GR1",
        "Occurrence Event Information_merged1",
        "flight_rule_merged1",
        "year_built_merged1",
        "amateur_built_merged1",
        "gear_type_merged1",
        "phase_of_flight_merged1",
        "phase_of_flight_merged1",
        "damage_merged1",
        "operator_type_merged1",
        "operator_type_merged1",
        "prcp",
        "snow",
    ],
    outputCol="features",
)
################### ML Model ####################################
classifier = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[str_indexer, assembler, classifier])
# pipeline = Pipeline(stages=[str_indexer])
model = pipeline.fit(train)

pred = model.transform(validation)
print(pred.show(20))
eval = pred.select(pred.label, pred.rawPrediction, pred.prediction)
evaluator = BinaryClassificationEvaluator()
score = evaluator.evaluate(eval)
print(score)

# pred.write.json("/content/gdrive/MyDrive/cadors_LR/")

"""SVM"""

# ################################# Pipeline ############################################
str_indexer1 = StringIndexer(
    inputCols=[
        "Occurrence Category_merged",
        "GR",
        "Occurrence Event Information_merged",
        "flight_rule_merged",
        "year_built_merged",
        "amateur_built_merged",
        "gear_type_merged",
        "phase_of_flight_merged",
        "damage_merged",
        "operator_type_merged",
    ],
    outputCols=[
        "Occurrence Category_merged1",
        "GR1",
        "Occurrence Event Information_merged1",
        "flight_rule_merged1",
        "year_built_merged1",
        "amateur_built_merged1",
        "gear_type_merged1",
        "phase_of_flight_merged1",
        "damage_merged1",
        "operator_type_merged1",
    ],
    handleInvalid="keep",
)
assembler1 = VectorAssembler(
    inputCols=[
        "hour",
        "month",
        "GR1",
        "Occurrence Category_merged1",
        "Occurrence Event Information_merged1",
        "flight_rule_merged1",
        "year_built_merged1",
        "amateur_built_merged1",
        "gear_type_merged1",
        "phase_of_flight_merged1",
        "phase_of_flight_merged1",
        "damage_merged1",
        "operator_type_merged1",
        "operator_type_merged1",
        "prcp",
        "snow",
    ],
    outputCol="features",
)
################### ML Model ####################################
classifier1 = LinearSVC(featuresCol="features", labelCol="label")
pipeline1 = Pipeline(stages=[str_indexer1, assembler1, classifier1])
# pipeline = Pipeline(stages=[str_indexer])
model1 = pipeline1.fit(train)

pred1 = model1.transform(validation)
print(pred1.show(20))
eval1 = pred1.select(pred1.label, pred1.rawPrediction, pred1.prediction)
evaluator1 = BinaryClassificationEvaluator()
score1 = evaluator1.evaluate(eval1)
print(score1)

# target_df = predictions.select([col('`{}`'.format(c)).cast(StringType()).alias(c) for c in predictions.columns])
temp = spark.read.json("s3://cadorscmpt732/processed_data/")
temp.write.option("header", True).csv("s3://cadorscmpt732/ml-classification/")


predictions = spark.read.json("/s3://cadorscmpt732/ml-classification/")
predictions.count()

temp = target_df.filter((col("Occurrence Category_merged") != "NA"))

temp.filter(col("prediction") == 1).show()
predictions.filter(col("prediction") == 1).show()

target_df = predictions.select(
    [col("`{}`".format(c)).cast(StringType()).alias(c) for c in predictions.columns]
)
# target_df = predictions.sample(withReplacement=False, fraction=0.10).select([col('`{}`'.format(c)).cast(StringType()).alias(c) for c in predictions.columns])

temp.write.option("header", True).csv("s3://cadorscmpt732/ml-classification/")

print(predictions.count())
print(
    "True Positive: ",
    predictions.filter((col("prediction") == 1) & (col("label") == 1)).count(),
)
print(
    "True Negative: ",
    predictions.filter((col("prediction") == 0) & (col("label") == 0)).count(),
)
print(
    "False Positive: ",
    predictions.filter((col("prediction") == 1) & (col("label") == 0)).count(),
)
print(
    "False Negative: ",
    predictions.filter((col("prediction") == 0) & (col("label") == 1)).count(),
)
job.commit()
