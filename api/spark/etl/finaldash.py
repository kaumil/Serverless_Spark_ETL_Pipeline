import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import *
from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.sparkContext.setLogLevel("WARN")
job = Job(glueContext)
job.init(args["JOB_NAME"], args)
## @type: DataSource
## @args: [database = "cadorsdw", table_name = "rawdata", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
# datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "cadorsdw", table_name = "rawdata", transformation_ctx = "datasource0")


sc = spark.sparkContext
data1 = spark.read.json("s3://cadorscmpt732/processed_data/")

data1 = data1.withColumn("# incidents", lit(1)).cache()
data1 = data1.na.drop(
    subset=[
        "aircraft_category",
        "make",
        "operator_type",
        "phase_of_flight",
        "year_built",
    ]
)
data1 = (
    data1.withColumn("phase_of_flight", regexp_replace("phase_of_flight", "'", ""))
    .withColumn("aircraft_category", regexp_replace("aircraft_category", "'", ""))
    .withColumn("Province:", regexp_replace("Province:", "'", ""))
    .withColumn("make", regexp_replace("make", "'", ""))
    .withColumn("operator_type", regexp_replace("operator_type", "'", ""))
    .withColumn("Occurrence Type:", regexp_replace("Occurrence Type:", "'", ""))
)

occ_cat = data1.select(data1["CADORS Number"], explode(data1["Occurrence Category"]))
occ_cat = occ_cat.toDF("CADORS Number", "Occurrence Category")
occ_cat = occ_cat.withColumnRenamed("CADORS Number", "Cadors_number")
occ_cat = occ_cat.withColumnRenamed("Occurrence Category", "occurrence_category")
occ_cat1 = occ_cat.join(
    data1, data1["CADORS Number"] == occ_cat["Cadors_number"], how="left"
)

# 1
date_df = data1.select(
    data1["CADORS Number"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
    data1["year"],
    data1["month"],
)
date_df = date_df.dropDuplicates()
# date_df = date_df.filter((date_df['year']!="NA") & (date_df['month']!="NA"))
date_df = date_df.groupBy("year", "month").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 2
phaseofflight_df = data1.select(
    data1["CADORS Number"],
    data1["phase_of_flight"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
)
phaseofflight_df = phaseofflight_df.dropDuplicates()
phaseofflight_df = phaseofflight_df.filter(phaseofflight_df["phase_of_flight"] != "NA")
phaseofflight_df = phaseofflight_df.groupBy("phase_of_flight").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 3
aircraftcategory_df = data1.select(
    data1["CADORS Number"],
    data1["aircraft_category"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
)
aircraftcategory_df = aircraftcategory_df.dropDuplicates()
aircraftcategory_df = aircraftcategory_df.filter(
    aircraftcategory_df["aircraft_category"] != "NA"
)
aircraftcategory_df = aircraftcategory_df.groupBy("aircraft_category").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 4
occcat_df = occ_cat1.select(
    occ_cat1["CADORS Number"],
    occ_cat1["occurrence_category"],
    occ_cat1["# incidents"],
    occ_cat1["Injuries:"],
    occ_cat1["Fatalities:"],
)
occcat_df = occcat_df.dropDuplicates()
occcat_df = occcat_df.filter(occcat_df["occurrence_category"] != "NA")
occcat_df = occcat_df.groupBy("occurrence_category").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 5
province_df = data1.select(
    data1["CADORS Number"],
    data1["Province:"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
    data1["Country:"],
)
province_df = province_df.dropDuplicates()
province_df = province_df.filter(province_df["Province:"] != "NA").filter(
    province_df["Country:"] != "NA"
)
province_df = province_df.groupBy("Province:", "Country:").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 6
CAprovince_df = data1.filter(province_df["Country:"] == "Canada")
CAprovince_df = CAprovince_df.dropDuplicates()
CAprovince_df = CAprovince_df.filter(CAprovince_df["Province:"] != "NA").filter(
    CAprovince_df["Country:"] != "NA"
)
CAprovince_df = CAprovince_df.groupBy("Province:", "Country:").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 7
make_df = data1.select(
    data1["CADORS Number"],
    data1["make"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
)
make_df = make_df.dropDuplicates()
# make_df = make_df.filter(make_df['make']!="NA")
make_df = make_df.groupBy("make").sum("# incidents", "Injuries:", "Fatalities:")
make_df = make_df.withColumn("make", regexp_replace("make", "'", ""))

# 8
yearbuilt_df = data1.select(
    data1["CADORS Number"],
    data1["year_built"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
)
yearbuilt_df = yearbuilt_df.dropDuplicates()
# yearbuilt_df = yearbuilt_df.filter(yearbuilt_df['year_built']!="NA")
yearbuilt_df = yearbuilt_df.groupBy("year_built").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 9
operatortype_df = data1.select(
    data1["CADORS Number"],
    data1["operator_type"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
)
operatortype_df = operatortype_df.dropDuplicates()
operatortype_df = operatortype_df.groupBy("operator_type").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 10
latlong_df = data1.select(
    data1["CADORS Number"],
    data1["longitude"],
    data1["latitude"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
)
latlong_df = latlong_df.dropDuplicates()
latlong_df = latlong_df.groupBy("latitude", "longitude").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 11
occtype_df = data1.select(
    data1["CADORS Number"],
    data1["Occurrence Type:"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
)
occtype_df = occtype_df.dropDuplicates()
occtype_df = occtype_df.groupBy("Occurrence Type:").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

# 12
mappers_df = data1.select(
    data1["CADORS Number"],
    data1["Country:"],
    data1["Province:"],
    data1["Fatalities:"],
    data1["Injuries:"],
    data1["# incidents"],
    data1["year"],
    data1["month"],
    data1["longitude"],
    data1["latitude"],
)
mappers_df = mappers_df.filter(mappers_df["Country:"] == "Canada")
mappers_df = mappers_df.dropDuplicates()
mappers_df = mappers_df.filter(mappers_df["Province:"] != "NA").filter(
    mappers_df["Country:"] != "NA"
)
mappers_df = mappers_df.groupby(
    "Province:", "year", "month", "Country:", "longitude", "latitude"
).sum("# incidents", "Injuries:", "Fatalities:")

# 13
mappercountry_df = data1.dropDuplicates()
mappercountry_df = mappercountry_df.filter(
    mappercountry_df["Province:"] != "NA"
).filter(mappercountry_df["Country:"] != "NA")
mappercountry_df = mappercountry_df.groupby(
    "Country:", "Province:", "year", "month", "longitude", "latitude"
).sum("# incidents", "Injuries:", "Fatalities:")

# 14
CAlatlong_df = data1.select(
    data1["CADORS Number"],
    data1["longitude"],
    data1["latitude"],
    data1["# incidents"],
    data1["Injuries:"],
    data1["Fatalities:"],
    data1["Country:"],
)
CAlatlong_df = CAlatlong_df.filter(CAlatlong_df["Country:"] == "Canada")

CAlatlong_df = CAlatlong_df.dropDuplicates()
CAlatlong_df = CAlatlong_df.groupBy("latitude", "longitude", "Country:").sum(
    "# incidents", "Injuries:", "Fatalities:"
)

date_df.coalesce(1).write.csv("s3://cadorscmpt732/quicksight/date_df/", header=True)
phaseofflight_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/phaseofflight_df/", header=True
)
aircraftcategory_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/aircraftcategory_df/", header=True
)
occcat_df.coalesce(1).write.csv("s3://cadorscmpt732/quicksight/occcat_df/", header=True)
province_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/province_df/", header=True
)
CAprovince_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/CAprovince_df/", header=True
)
make_df.coalesce(1).write.csv("s3://cadorscmpt732/quicksight/make_df/", header=True)
yearbuilt_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/yearbuilt_df/", header=True
)
operatortype_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/operatortype_df/", header=True
)
latlong_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/latlong_df/", header=True
)
occtype_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/occtype_df/", header=True
)
mappers_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/mappers_df/", header=True
)
mappercountry_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/mappercountry_df/", header=True
)
CAlatlong_df.coalesce(1).write.csv(
    "s3://cadorscmpt732/quicksight/CAlatlong_df/", header=True
)
job.commit()
