from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import count, col, lower,regexp_replace

# Initialize Spark Session
spark = SparkSession.builder.appName("company_unique_data").getOrCreate()

# add the parquet file to a data frame
company_df = spark.read.format("parquet").load("veridion_entity_resolution_challenge.snappy.parquet")\
             .sortWithinPartitions("company_name")
#lowercase the company names and remove punctuation
company_df = company_df.withColumn("company_name",regexp_replace(lower(col("company_name")),r"[^\w\s]", "").alias("company_name"))
#we partition by country code
company_df.write.partitionBy("main_country_code").mode("overwrite").parquet("/tmp/parquet_data")

# create a data frame which has a non null combination of company name and country code
unique_company_df = company_df.filter(col("company_name").isNotNull()).filter(col("main_country_code").isNotNull())
# add the rows which dont't have a non-null combination of company name and country code
non_unique_company_df = company_df.exceptAll(unique_company_df).sortWithinPartitions("company_name")
# print(f"Results here: {non_unique_company_df.count()}")
# do the deduplication process
# define window partitioned by country code and company name
window_spec = Window.partitionBy("company_name", "main_country_code")
# add a count column and filter for duplicates
unique_company_df_with_count = unique_company_df.withColumn("count", count("*").over(window_spec))
unique_company_duplicates_df = unique_company_df_with_count.filter(col("count") > 1).drop("count")
unique_company_non_duplicates_df = unique_company_df_with_count.filter(col("count") == 1).drop("count")

#now we can analyse the remaining data
# edge case null company name
null_company_df = non_unique_company_df.filter(col("company_name").isNull())
# get the rows with a non null website
non_null_website_null_company_df = null_company_df.filter(col("website_domain").isNotNull()).sortWithinPartitions("website_domain")
#get distinct website
distinct_non_null_website_null_company_df = non_null_website_null_company_df.select("website_domain").distinct()
#we want to see which of these distinct websites are not in unique company
distinct_non_null_website_null_company_not_in_not_unique_company_df = distinct_non_null_website_null_company_df.join(unique_company_df, on="website_domain", how="left_anti")
# and use this to filter non_null_website_null_company_df - these websites do not come up in the unique company data
non_null_website_null_company_filtered_df = non_null_website_null_company_df.filter(col("website_domain")\
                                           .isin(distinct_non_null_website_null_company_not_in_not_unique_company_df))
#get the rows which have a null company and website - these will be added to the duplicates array
null_website_null_company_filtered_df = null_company_df.exceptAll(non_null_website_null_company_filtered_df)
# edge case company is not null
non_null_company_df = non_unique_company_df.filter(col("company_name").isNotNull())
#get distinct company
distinct_non_null_company_df = non_null_company_df.select("company_name").distinct()
#we want to see which of these distinct companies are not in unique company
distinct_non_null_company_not_in_not_unique_company_df = distinct_non_null_company_df.join(unique_company_df, on="company_name", how="left_anti")
# and use this to filter non_null_company_df - these companies do not come up in the unique company data
non_null_company_filtered_df = non_null_company_df.filter(col("company_name")\
                              .isin(distinct_non_null_company_not_in_not_unique_company_df))\
                               .dropDuplicates(["company_name"])

#get the rows which have a non null company but are found in the company data - these will be added to the duplicates array
non_null_company_filtered_df_dupes = non_null_company_df.exceptAll(non_null_company_filtered_df)

#bring it all together

final_non_duplicated_company_df = unique_company_non_duplicates_df.union(non_null_website_null_company_filtered_df)\
                                  .union(non_null_company_filtered_df)
final_duplicated_company_df = unique_company_duplicates_df.union(null_website_null_company_filtered_df)\
                              .union(non_null_company_filtered_df_dupes)
#print(f"Non duplicated results count: {final_duplicated_company_df.count()}")
final_non_duplicated_company_df.show(truncate=False,vertical=True)
spark.stop()