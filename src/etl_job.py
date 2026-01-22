from pyspark.sql.functions import col, lit, when, sum as _sum, count, to_date, row_number, first, udf, input_file_name, regexp_extract, substring,translate, year, add_months
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, sum as _sum, count, to_date, row_number, first, udf
from pyspark.sql.types import StringType, TimestampType  
from pyspark.sql.window import Window
import os

# --- CẤU HÌNH (SỬA LẠI ĐƯỜNG DẪN CỦA BẠN) ---
# 1. Đường dẫn file JAR vừa tải (QUAN TRỌNG)
JDBC_JAR_PATH = "/app/postgresql-42.7.2.jar"

# 2. Đường dẫn Data thô (Raw Data)
PATH_LOG_CONTENT = "/data_lake/log_content"
PATH_LOG_SEARCH = "/data_lake/log_search"

# 3. Thông tin kết nối Database Docker
DB_URL = "jdbc:postgresql://postgres_dw:5432/customer_dw"
DB_PROPS = {
    "user": "admin",
    "password": "password123",
    "driver": "org.postgresql.Driver"
}

# Khởi tạo Spark
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

# ====================================================
# 1. XỬ LÝ LOG CONTENT (XEM PHIM/TRUYỀN HÌNH)
# ====================================================
def process_log_content():
    print("\n=== BAT DAU XU LY LOG CONTENT ===")
    
    # Đọc dữ liệu JSON
    df = spark.read.json(PATH_LOG_CONTENT)
    
    df = df.withColumn("filename_date", regexp_extract(input_file_name(), r"(\d{8})", 1))

  
    # Flatten & Chọn cột
    df = df.select(
        col("_source.Contract").alias("customer_key"),
        col("_source.AppName").alias("AppName"),
        col("_source.TotalDuration").alias("total_duration"),
        to_date(col("filename_date"), "yyyyMMdd").alias("date_key") 
    ).filter(col("customer_key") != '0')

    # Logic Mapping AppName -> Type Name (Tiếng Việt)
    df = df.withColumn("type_name", 
        when(col("AppName") == "CHANNEL", "Truyen Hinh")
        .when(col("AppName") == "RELAX", "Giai Tri")
        .when(col("AppName") == "CHILD", "Thieu Nhi")
        .when(col("AppName").isin("FIMS", "VOD"), "Phim Truyen")
        .when(col("AppName").isin("KPLUS", "SPORT"), "The Thao")
        .otherwise("Khac")
    )

    # --- Bước A: Cập nhật Dim_Customer (Tính sở thích) ---
    # Tính tổng thời gian xem theo từng loại của mỗi khách
    window_spec = Window.partitionBy("customer_key").orderBy(col("sum_duration").desc())
    
    user_profile = df.groupBy("customer_key", "type_name") \
        .agg(_sum("total_duration").alias("sum_duration"))
    
    # Lấy loại xem nhiều nhất (Rank 1)
    user_profile = user_profile.withColumn("rank", row_number().over(window_spec)) \
        .filter(col("rank") == 1) \
        .select("customer_key", col("type_name").alias("favorite_genre"))
    
    # Tạo thêm cột segment (Giả định logic Active)
    # Ở đây để đơn giản ta gán mặc định, logic phức tạp hơn có thể thêm sau
    final_customer = user_profile.withColumn("taste_profile", col("favorite_genre")) \
                                 .withColumn("activity_segment", lit("High")) \
                                 .withColumn("last_updated", lit(None).cast(TimestampType())) 
    
    # Ghi đè Dim_Customer (Full Load để cập nhật mới nhất)
    # Lưu ý: Thực tế nên dùng Upsert, nhưng project này ta dùng Overwrite cho đơn giản
    print("--> Dang kiem tra khach hang moi...")
    try:
        # 1. Đọc danh sách khách hàng ĐANG CÓ trong Database
        existing_customers = read_db_table("dim_customer").select("customer_key")
        
        # 2. Tìm khách hàng MỚI (Lấy tập Final trừ đi tập Existing)
        # Dùng left_anti join: Giữ lại những ông bên trái KHÔNG CÓ mặt bên phải
        new_customers = final_customer.join(existing_customers, on="customer_key", how="left_anti")
        
        # 3. Chỉ ghi những người mới vào (Mode Append)
        if new_customers.count() > 0:
            print(f"--> Phat hien {new_customers.count()} khach hang moi. Dang ghi vao DB...")
            write_to_db(new_customers, "dim_customer", mode="append")
        else:
            print("--> Khong co khach hang moi nao.")
            
    except Exception as e:
        # Trường hợp chạy lần đầu tiên (Bảng trống trơn) hoặc lỗi khác
        print(f"--> Ghi truc tiep do bang co the dang rong. Chi tiet: {str(e)}")
        write_to_db(final_customer, "dim_customer", mode="append") # Cần cẩn thận khóa ngoại

    # --- Bước B: Xử lý Fact_Watch_Activity ---
    # Lấy bảng dim_content_type từ DB để map ra ID (Thay vì lưu text)
    dim_content = read_db_table("dim_content_type")
    
    # Join để lấy content_type_id
    fact_watch = df.join(dim_content, on="type_name", how="left")
    
    # Group lại theo ngày để giảm dòng (Aggregation)
    fact_watch_final = fact_watch.groupBy("date_key", "customer_key", "content_type_id") \
        .agg(_sum("total_duration").alias("total_duration")) \
        .na.drop(subset=["customer_key", "date_key", "content_type_id"]) # Bỏ dòng null
        
    write_to_db(fact_watch_final, "fact_watch_activity", mode="append")

# ====================================================
# 2. XỬ LÝ LOG SEARCH (TÌM KIẾM)
# ====================================================
def process_log_search():
    print("\n=== BAT DAU XU LY LOG SEARCH ===")
    
    # Đọc Parquet (đệ quy folder)
    # Lưu ý: Nếu lỗi path, hãy trỏ thẳng vào 1 folder ngày cụ thể để test
    df = spark.read.option("mergeSchema", "true").parquet(os.path.join(PATH_LOG_SEARCH, "*"))
    
# Bộ số cần dịch
    src_digits = "۰۱۲۳۴۵۶۷۸۹٠١٢٣٤٥٦٧٨٩০১২৩৪৫৬৭৮৯"
    dst_digits = "012345678901234567890123456789"

    # Bước 1: Dịch số và tạo cột ngày tạm
    df_step1 = df.select(
        col("user_id").alias("customer_key"),
        substring(col("keyword"), 0, 255).alias("original_keyword"),
        to_date(translate(col("datetime"), src_digits, dst_digits)).alias("temp_date")
    ).filter(col("original_keyword").isNotNull())

    # Bước 2: Xử lý năm Thái Lan + LỌC DỮ LIỆU RÁC (Năm 0004...)
    df_clean = df_step1.withColumn("date_key", 
        when(year(col("temp_date")) > 2400, add_months(col("temp_date"), -6516)) # Trừ năm Thái
        .otherwise(col("temp_date"))
    ).filter(year(col("date_key")) > 2000) # <--- CHỐT CHẶN: Chỉ lấy dữ liệu sau năm 2000

    # --- Bước A: Xử lý Dim_Keyword (Giả lập AI) ---
    # Lấy danh sách từ khóa mới
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
    
    # Lưu vào Dim_Keyword (Dùng append, bỏ qua lỗi trùng lặp nếu có - thực tế cần xử lý khéo hơn)
    # Cách đơn giản nhất cho bài lab: Đọc bảng cũ, lọc cái chưa có, rồi insert
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

    # --- SỬA LẠI: Tự động thêm khách hàng từ Search vào Dim_Customer ---
    
    print("--> Dang kiem tra va them khach hang tu Log Search vao DB...")
    
    # 1. Lấy danh sách ID khách hàng duy nhất từ file Search
    search_user_ids = fact_search_final.select("customer_key").distinct()
    
    # 2. Tạo thông tin giả (Dummy info) cho họ (vì log search không có thông tin này)
    # Lưu ý: Import TimestampType ở đầu file nếu chưa có
    new_search_customers = search_user_ids.withColumn("favorite_genre", lit("Unknown")) \
                                          .withColumn("taste_profile", lit("Unknown")) \
                                          .withColumn("activity_segment", lit("Search Only")) \
                                          .withColumn("last_updated", lit(None).cast(TimestampType()))

    # 3. Lọc: Chỉ lấy những ông CHƯA CÓ trong DB (tránh trùng lặp)
    existing_customers = read_db_table("dim_customer").select("customer_key")
    customers_to_add = new_search_customers.join(existing_customers, on="customer_key", how="left_anti")
    
    # 4. Ghi những ông mới này vào bảng Dim_Customer
    if customers_to_add.count() > 0:
        print(f"--> Phat hien {customers_to_add.count()} khach hang Search moi. Dang them vao dim_customer...")
        write_to_db(customers_to_add, "dim_customer", mode="append")
    else:
        print("--> Tat ca khach hang Search da ton tai trong DB.")

    # 5. Cuối cùng: Ghi dữ liệu Search vào Fact (Giờ thì khóa ngoại đã an toàn!)
    print("--> Dang ghi vao bang: fact_search_activity...")
    write_to_db(fact_search_final, "fact_search_activity", mode="append")
        

# ====================================================
# MAIN
# ====================================================
if __name__ == "__main__":
    # 1. Chạy xử lý Content trước (để có Dim Customer)
    process_log_content()
    
    # 2. Chạy xử lý Search
    process_log_search()
    
    print("\n---------------------------------------")
    print("ALL JOBS FINISHED SUCCESSFULLY!")
    spark.stop()