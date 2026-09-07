from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from spark_utils import parse_args, clean_meteo, clean_eco2mix, clean_holiday

def main() -> None:
    args = parse_args()
    spark = SparkSession.builder.appName("silver_layer").config("spark.sql.sources.partitionOverwriteMode", "dynamic").getOrCreate()


    eco2mix_df = spark.read.parquet(args.eco2mix_path)
    meteo_df = spark.read.parquet(args.openmeteo_path)

    if args.start_date:
        eco2mix_df = eco2mix_df.filter(F.col("date_heure") >= args.start_date)
        meteo_df = meteo_df.filter(F.col("time") >= args.start_date)

    clean_eco2mix_df = clean_eco2mix(eco2mix_df)
    clean_meteo_df = clean_meteo(meteo_df)

    result_df = clean_eco2mix_df.join(clean_meteo_df, on=["date_heure", "region_code"], how="left")

    if args.holiday_path:
        holiday_df = spark.read.parquet(args.holiday_path)
        if args.start_date:
            holiday_df = holiday_df.filter(F.col("date") >= args.start_date)
        clean_holiday_df = clean_holiday(holiday_df)
        result_df = (
            result_df
            .withColumn("date", F.to_date("date_heure"))
            .join(clean_holiday_df, on=["date", "region_code"], how="left")
            .drop("date")
        )
    else:
        result_df = result_df.withColumn("is_holiday", F.lit(0))

    result_df = result_df.fillna({"is_holiday": 0})
    result_df = result_df.withColumn("year", F.year("date_heure"))

    result_df.write.partitionBy(
                "region_code", "year"
            ).parquet(args.output_dir, mode="overwrite")

if __name__ == "__main__":
    main()