from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_utils import parse_args, clean_meteo, clean_eco2mix

def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("silver_layer").config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate()
    

    eco2mix_df = spark.read.parquet(args.eco2mix_path)
    meteo_df = spark.read.parquet(args.openmeteo_path)
    holiday_df = spark.read.parquet(args.holiday_path)

    if args.start_date:
        eco2mix_df = eco2mix_df.filter(F.col("date_heure") >= args.start_date)
        meteo_df = meteo_df.filter(F.col("time") >= args.start_date)
        holiday_df = holiday_df.filter(F.col("date") >= args.start_date)

    clean_eco2mix_df = clean_eco2mix(eco2mix_df)
    clean_meteo_df = clean_meteo(meteo_df)
    clean_holiday_df = clean_meteo(holiday_df)

    join_df = clean_eco2mix_df.join(clean_meteo_df, on = ["date_heure", "region_code"], how="left")
    result_df = join_df.join(clean_holiday_df, on = ["date_heure", "region_code"], how="left")
    result_df = result_df.fillna(0)

    join_df = (
        join_df
        .withColumn("year", F.year("date_heure"))
    )

    join_df.write.partitionBy(
                "region_code", "year"
            ).parquet(args.output_dir, mode="overwrite")
    
if __name__ == "__main__":
    main()