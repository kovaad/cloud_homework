CREATE TABLE kovaad_homework.silver_views
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://kerekerdo/datalake/views_silver'
    ) AS SELECT article, views, rank, date
         FROM kovaad_homework.bronze_views 
         WHERE date IS NOT NULL;