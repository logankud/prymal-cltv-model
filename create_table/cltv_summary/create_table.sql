CREATE EXTERNAL TABLE IF NOT EXISTS shopify_cltv_rfm_inference(

email STRING
,frequency INT
,recency INT
, T INT
,monetary_value DOUBLE
, p_alive FLOAT
,n_predicted_purchases_60 FLOAT
, aov   DOUBLE
,predicted_clv DOUBLE


)
PARTITIONED BY 
(
partition_date DATE 
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LOCATION 's3://prymal-analytics/shopify/cltv/rfm_inference/' 
TBLPROPERTIES ("skip.header.line.count"="1")