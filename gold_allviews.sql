-- ADD THE ATHENA SQL SCRIPT HERE WHICH CREATES THE `gold_allviews` TABLE
 
 CREATE TABLE kovaad_homework.gold_allviews
    WITH (
          format = 'PARQUET',
          parquet_compression = 'SNAPPY',
          external_location = 's3://kerekerdo/datalake/gold_allviews'
    ) AS SELECT article, SUM(views) AS total_top_views, MIN(rank) AS top_rank, COUNT(date) AS ranked_days
         FROM kovaad_homework.silver_views 
         GROUP BY article;