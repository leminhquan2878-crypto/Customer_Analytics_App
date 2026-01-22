
CREATE TABLE dim_date (
    date_key DATE PRIMARY KEY,
    day INT,
    month INT,
    year INT,
    quarter INT,
    day_of_week INT,
    is_weekend BOOLEAN
);


CREATE TABLE dim_customer (
    customer_key VARCHAR(50) PRIMARY KEY, -- Contract hoặc User_ID
    taste_profile VARCHAR(255),
    favorite_genre VARCHAR(100),
    activity_segment VARCHAR(20),
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE dim_content_type (
    content_type_id SERIAL PRIMARY KEY,
    type_name VARCHAR(50) UNIQUE -- Truyen Hinh, The Thao...
);


CREATE TABLE dim_keyword (
    keyword_id SERIAL PRIMARY KEY,
    original_keyword VARCHAR(255) UNIQUE,
    genre_ai VARCHAR(100)
);


CREATE TABLE fact_watch_activity (
    fact_id SERIAL PRIMARY KEY,
    customer_key VARCHAR(50) REFERENCES dim_customer(customer_key),
    content_type_id INT REFERENCES dim_content_type(content_type_id),
    date_key DATE REFERENCES dim_date(date_key),
    total_duration BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE fact_search_activity (
    fact_id SERIAL PRIMARY KEY,
    customer_key VARCHAR(50) REFERENCES dim_customer(customer_key),
    keyword_id INT REFERENCES dim_keyword(keyword_id),
    date_key DATE REFERENCES dim_date(date_key),
    search_count INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


INSERT INTO dim_content_type (type_name) VALUES 
('Truyen Hinh'), ('Giai Tri'), ('Thieu Nhi'), 
('Phim Truyen'), ('The Thao'), ('Khac')
ON CONFLICT (type_name) DO NOTHING;


INSERT INTO dim_date
SELECT 
    datum AS date_key,
    EXTRACT(DAY FROM datum) AS day,
    EXTRACT(MONTH FROM datum) AS month,
    EXTRACT(YEAR FROM datum) AS year,
    EXTRACT(QUARTER FROM datum) AS quarter,
    EXTRACT(ISODOW FROM datum) AS day_of_week,
    CASE WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM (SELECT '2020-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 3650) AS SEQUENCE(DAY)) DQ
ORDER BY 1;