import sys
from pyspark.sql import SparkSession, functions, types
from pyspark.context import SparkContext
from pyspark.sql.types import IntegerType, StringType
from pyspark.sql.functions import *
from pyspark.sql.functions import isnan, when, count, col
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator


## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

spark.sparkContext.setLogLevel("WARN")
sc = spark.sparkContext

sc = SparkContext()
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


aircraft_df = spark.read.json("s3://cadorscmpt732/processed_files/*")
weather_df = spark.read.json("s3://cadorscmpt732/processed_files/weather/*")

aircraft_df = aircraft_df.na.drop(
    subset=[
        "aircraft_category",
        "make",
        "operator_type",
        "phase_of_flight",
        "year_built",
    ]
)

print(aircraft_df.count())
print(aircraft_df.select(["CADORS Number"]).distinct().count())

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

weather_df = weather_df.join(aircraft_df.select(["CADORS Number"]), ["CADORS Number"])
weather_df = weather_df.dropDuplicates()

print(weather_df.count())
print(weather_df.select(["CADORS Number"]).distinct().count())

"""CLUSTERING FOR ONLY ACCIDENTS"""


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
featureset1 = aircraft_df.select(
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
featureset2 = aircraft_df.groupBy("CADORS Number").agg(
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
featureset3 = weather_df.select(["CADORS Number", "prcp", "snow"]).dropDuplicates()
featureset3 = featureset3.dropna(how="any")
print("FEATURES 3\n")
print(featureset3.count())
print(featureset3.head(5))

## Final data for ML
ff = featureset1.join(featureset2, ["CADORS Number"], how="inner")
ff = ff.join(featureset3, ["CADORS Number"], how="inner")
print(ff.count())
print(ff.head(5))

######################### ML DATA ####################################
finalMLData = ff

for k in range(9, 10):
    # str_indexer = StringIndexer(inputCols=['Canadian Aerodrome ID:','Country:','TC Region:','Day Or Night:','World Area:','Occurrence Type:','Province:','Occurrence Event Information_merged','Occurrence Category_merged','aircraft_category_merged','flight_rule_merged','cars_subpart_merged'], outputCols=['Canadian Aerodrome ID1:','Country1:','TC Region1:','Day Or Night1:','World Area1:','Occurrence Type1:','Province1:','Occurrence Event Information_merged1','Occurrence Category_merged1','aircraft_category_merged1','flight_rule_merged1','cars_subpart_merged1'])
    # m=str_indexer.fit(finalfeatures)
    # td=m.transform(finalfeatures)
    # print(td.head())
    # print(str_indexer.transform(finalfeatures)).head()
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
    # print(assembler.transform(finalfeatures).head(5))
    kmeans = KMeans(k=k, seed=1, featuresCol="features", predictionCol="prediction")
    clustering_pipeline = Pipeline(stages=[str_indexer, assembler, kmeans])
    model = clustering_pipeline.fit(finalMLData)
    predictions = model.transform(finalMLData)
    # Evaluate clustering by computing Silhouette score
    evaluator = ClusteringEvaluator()
    silhouette = evaluator.evaluate(predictions)
    print(
        "Silhouette for k= ",
        k,
        " with squared euclidean distance = " + str(silhouette),
        "\n",
    )
    predictions.show()
# kmeans = KMeans(k=3,seed=1,featuresCol="features",predictionCol="prediction")
# clustering_pipeline = Pipeline(stages=[str_indexer,assembler,kmeans])
# model=clustering_pipeline.fit(finalfeatures)
# predictions = model.transform(finalfeatures)

val_data.printSchema()

val_data.groupby("prediction").agg(
    sum(col("Fatalities:")), sum(col("Injuries:"))
).show()

predictions = spark.read.json("s3://cadorscmpt732/ml-classification/")
aircraft_df = spark.read.json("s3://cadorscmpt732/processed_files/")

sample = predictions.select(
    [
        "CADORS Number",
        "GR1",
        "Occurrence Category_merged1",
        "Occurrence Event Information_merged1",
        "amateur_built_merged1",
        "damage_merged1",
        "flight_rule_merged1",
        "gear_type_merged1",
        "hour",
        "month",
        "operator_type_merged1",
        "phase_of_flight_merged1",
        "prcp",
        "snow",
        "year_built_merged1",
        "prediction",
    ]
)

sample.write.option("header", True).csv("s3://cadorscmpt732/ml-classification/")

val_data = predictions.select(["CADORS Number", "prediction"]).join(
    aircraft_df, ["CADORS Number"]
)
val_data = val_data.dropDuplicates()

val_data = val_data.select(
    ["CADORS Number", "Injuries:", "Fatalities:", "prediction"]
).dropDuplicates()
print(val_data.count())
print(val_data.select("CADORS Number").distinct().count())

sample_clustering = (
    val_data.sample(withReplacement=False, fraction=0.01)
    .write.option("header", True)
    .csv("/content/gdrive/MyDrive/cadorsdata_K9_sample")
)

# val_data.show()
temp = val_data.groupby("prediction").agg(
    countDistinct("CADORS Number"),
    sum("Injuries:"),
    sum("Fatalities:"),
    sum("Injuries:") + sum("Fatalities:").alias("totalharm"),
    (
        (sum("Injuries:") + sum("Fatalities:")) * 100 / countDistinct("CADORS Number")
    ).alias("harmppercent"),
)
temp.sort(["harmppercent"], ascending=[False]).write.option("header", True).csv(
    "s3://cadorscmpt732/ml-classification/"
)


datasink4 = glueContext.write_dynamic_frame.from_catalog(
    frame=resolvechoice3,
    database="cadorsdw",
    table_name="kpirawdatatest",
    transformation_ctx="datasink4",
)
job.commit()
