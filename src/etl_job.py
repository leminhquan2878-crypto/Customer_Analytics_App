from pyspark.sql.functions import col, lit, when, sum as _sum, count, to_date, row_number, first, udf, input_file_name, regexp_extract, substring,translate, year, add_months
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum as _sum, count, to_date, row_number, first, udf
from pyspark.sql.types import StringType, TimestampType  
from pyspark.sql.window import Window
import os

JDBC_JAR_PATH = "/app/postgresql-42.7.2.jar"
PATH_LOG_CONTENT = "/data_lake/log_content"
PATH_LOG_SEARCH = "/data_lake/log_search"
DB_URL = "jdbc:postgresql://postgres_dw:5432/customer_dw"
DB_PROPS = {
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

spark = SparkSession.builder \
    .appName("CustomerAnalyticsETL") \
    .config("spark.jars", JDBC_JAR_PATH) \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

def read_db_table(table_name):
    """Hàm đọc bảng từ Postgres lên để tham chiếu ID"""
    return spark.read.jdbc(url=DB_URL, table=table_name, properties=DB_PROPS)

def write_to_db(df, table_name, mode="append"):
    """Hàm ghi dữ liệu vào Postgres"""
    print(f"--> Dang ghi vao bang: {table_name}...")
    df.write.jdbc(url=DB_URL, table=table_name, mode=mode, properties=DB_PROPS)
    print(f"--> Ghi xong {table_name}!")

def process_log_content():
    print("\n=== BAT DAU XU LY LOG CONTENT ===")
    
    # Đọc dữ liệu JSON
    df = spark.read.json(PATH_LOG_CONTENT)
    
    df = df.withColumn("filename_date", regexp_extract(input_file_name(), r"(\d{8})", 1))

    df = df.select(
        col("_source.Contract").alias("customer_key"),
        col("_source.AppName").alias("AppName"),
        col("_source.TotalDuration").alias("total_duration"),
        to_date(col("filename_date"), "yyyyMMdd").alias("date_key") 
    ).filter(col("customer_key") != '0')

    df = df.withColumn("type_name", 
        when(col("AppName") == "CHANNEL", "Truyen Hinh")
        .when(col("AppName") == "RELAX", "Giai Tri")
        .when(col("AppName") == "CHILD", "Thieu Nhi")
        .when(col("AppName").isin("FIMS", "VOD"), "Phim Truyen")
        .when(col("AppName").isin("KPLUS", "SPORT"), "The Thao")
        .otherwise("Khac")
    )
    window_spec = Window.partitionBy("customer_key").orderBy(col("sum_duration").desc())
    user_profile = df.groupBy("customer_key", "type_name") \
        .agg(_sum("total_duration").alias("sum_duration"))
    user_profile = user_profile.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1) \
        .select("customer_key", col("type_name").alias("favorite_genre"))
    
    final_customer = user_profile.withColumn("taste_profile", col("favorite_genre")) \
                                 .withColumn("activity_segment", lit("High")) \
                                 .withColumn("last_updated", lit(None).cast(TimestampType())) 
    print("--> Dang kiem tra khach hang moi...")
    try:
        existing_customers = read_db_table("dim_customer").select("customer_key")
        new_customers = final_customer.join(existing_customers, on="customer_key", how="left_anti")
        if new_customers.count() > 0:
            print(f"--> Phat hien {new_customers.count()} khach hang moi. Dang ghi vao DB...")
            write_to_db(new_customers, "dim_customer", mode="append")
        else:
            print("--> Khong co khach hang moi nao.")
            
    except Exception as e:
        print(f"--> Ghi truc tiep do bang co the dang rong. Chi tiet: {str(e)}")
        write_to_db(final_customer, "dim_customer", mode="append") # Cần cẩn thận khóa ngoại

    dim_content = read_db_table("dim_content_type")
    fact_watch = df.join(dim_content, on="type_name", how="left")
    fact_watch_final = fact_watch.groupBy("date_key", "customer_key", "content_type_id") \
        .agg(_sum("total_duration").alias("total_duration")) \
        .na.drop(subset=["customer_key", "date_key", "content_type_id"]) # Bỏ dòng null
        
    write_to_db(fact_watch_final, "fact_watch_activity", mode="append")

def process_log_search():
    print("\n=== BAT DAU XU LY LOG SEARCH ===")
    df = spark.read.option("mergeSchema", "true").parquet(os.path.join(PATH_LOG_SEARCH, "*"))

    src_digits = "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩০১২৩৪৫৬৭৮৯"
    dst_digits = "012345678901234567890123456789"
    df_step1 = df.select(
        col("user_id").alias("customer_key"),
        substring(col("keyword"), 0, 255).alias("original_keyword"),
        to_date(translate(col("datetime"), src_digits, dst_digits)).alias("temp_date")
    ).filter(col("original_keyword").isNotNull())
    df_clean = df_step1.withColumn("date_key", 
        when(year(col("temp_date")) > 2400, add_months(col("temp_date"), -6516)) # Trừ năm Thái
        .otherwise(col("temp_date"))
    ).filter(year(col("date_key")) > 2000) # <--- CHỐT CHẶN: Chỉ lấy dữ liệu sau năm 2000
    
    unique_keywords = df_clean.select("original_keyword").distinct()
    
    # Ở đây ta giả lập hàm AI bằng UDF đơn giản (Để code chạy được ngay)
    # Bạn có thể thay bằng logic gọi OpenAI API thật của bạn sau
    def simple_classify(kw):
        kw = kw.lower()
        if "phim" in kw: return "Movie"
        if "bong da" in kw or "u23" in kw: return "Sports"
        if "nhac" in kw: return "Music"
        return "General" # Mặc định
        
    classify_udf = udf(simple_classify, StringType())
    
    dim_keyword_df = unique_keywords.withColumn("genre_ai", classify_udf(col("original_keyword")))

    try:
        existing_kw = read_db_table("dim_keyword").select("original_keyword")
        new_kw = dim_keyword_df.join(existing_kw, on="original_keyword", how="left_anti")
        if new_kw.count() > 0:
            write_to_db(new_kw, "dim_keyword", mode="append")
    except:
        # Nếu bảng chưa có gì
        write_to_db(dim_keyword_df, "dim_keyword", mode="append")

    # --- Bước B: Xử lý Fact_Search_Activity ---
    # Lấy ID từ bảng Dim vừa cập nhật
    dim_kw_db = read_db_table("dim_keyword")
    
    fact_search = df_clean.join(dim_kw_db, on="original_keyword", how="inner")
    
    fact_search_final = fact_search.filter(col("customer_key").isNotNull()) \
                                   .groupBy("date_key", "customer_key", "keyword_id") \
                                   .agg(count("original_keyword").alias("search_count"))
    
    print("--> Dang kiem tra va them khach hang tu Log Search vao DB...")

    search_user_ids = fact_search_final.select("customer_key").distinct()
    
    new_search_customers = search_user_ids.withColumn("favorite_genre", lit("Unknown")) \
                                          .withColumn("taste_profile", lit("Unknown")) \
                                          .withColumn("activity_segment", lit("Search Only")) \
                                          .withColumn("last_updated", lit(None).cast(TimestampType()))

    existing_customers = read_db_table("dim_customer").select("customer_key")
    customers_to_add = new_search_customers.join(existing_customers, on="customer_key", how="left_anti")

    if customers_to_add.count() > 0:
        print(f"--> Phat hien {customers_to_add.count()} khach hang Search moi. Dang them vao dim_customer...")
        write_to_db(customers_to_add, "dim_customer", mode="append")
    else:
        print("--> Tat ca khach hang Search da ton tai trong DB.")

    # 5. Cuối cùng: Ghi dữ liệu Search vào Fact (Giờ thì khóa ngoại đã an toàn!)
    print("--> Dang ghi vao bang: fact_search_activity...")
    write_to_db(fact_search_final, "fact_search_activity", mode="append")
        

if __name__ == "__main__":

    process_log_content()

    process_log_search()
    
    print("\n---------------------------------------")
    print("ALL JOBS FINISHED SUCCESSFULLY!")
    spark.stop()
