Create storage integration aws_intg
type=external_stage
enabled=true
storage_provider=s3
storage_allowed_locations=('s3://ndataassignment/')
storage_aws_role_arn='arn:aws:iam::953480402122:role/lamdaval-Neha';

desc storage integration aws_intg;

use database SNOWFLAKE_LEARNING_DB;

create stage aws_stage
url='s3://ndataassignment/'
storage_integration=aws_intg;

ls @aws_stage;

-- CREATE OR REPLACE TABLE raw_bank_holidays (data VARIANT);

-- COPY INTO raw_bank_holidays
-- FROM @aws_stage/bank-holidays.json
-- FILE_FORMAT = (TYPE = 'JSON');

-- CREATE OR REPLACE TABLE raw_hw_data (
--   height_in FLOAT,
--   weight_lb FLOAT
-- );

-- COPY INTO raw_hw_data
-- FROM @aws_stage/hw_200.csv
-- FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);

-- CREATE OR REPLACE VIEW view_raw_bank_holidays AS
-- SELECT * FROM raw_bank_holidays;

-- CREATE OR REPLACE VIEW view_raw_hw_data AS
-- SELECT * FROM raw_hw_data;

-- CREATE OR REPLACE VIEW view_transformed_bank_holidays AS
-- SELECT 
--     flattened_event.value:title::STRING AS holiday_name,
--     TO_DATE(flattened_event.value:date::STRING, 'YYYY-MM-DD') AS holiday_date,
--     flattened_event.value:bunting::BOOLEAN AS has_bunting
-- FROM raw_bank_holidays
--     , LATERAL FLATTEN(
--         input => data['england-and-wales']['events']
--       ) AS flattened_event;

-- CREATE OR REPLACE VIEW view_transformed_hw_data AS
-- SELECT 
--   height_in,
--   weight_lb,
--   ROUND(weight_lb / POWER(height_in, 2) * 703, 2) AS bmi
-- FROM raw_hw_data;

SELECT * FROM view_transformed_bank_holidays;
SELECT * FROM view_transformed_bank_holidays LIMIT 10;


CREATE OR REPLACE TABLE raw_bank_holidays (data VARIANT);

COPY INTO raw_bank_holidays
FROM @aws_stage/holidays_7ed71e49-630f-4be3-bfdc-c88c9d1332f4.json
FILE_FORMAT = (TYPE = 'JSON');

CREATE OR REPLACE TABLE raw_hw_data (data VARIANT);

COPY INTO raw_hw_data
FROM @aws_stage/height_weight_7ed71e49-630f-4be3-bfdc-c88c9d1332f4.json
FILE_FORMAT = (TYPE = 'JSON');
CREATE OR REPLACE VIEW view_raw_bank_holidays AS
SELECT * FROM raw_bank_holidays;

CREATE OR REPLACE VIEW view_raw_hw_data AS
SELECT * FROM raw_hw_data;

CREATE OR REPLACE VIEW view_transformed_bank_holidays AS
SELECT 
  value:title::STRING AS holiday_name,
  TO_DATE(value:date::STRING, 'YYYY-MM-DD') AS holiday_date,
  value:bunting::BOOLEAN AS has_bunting
FROM raw_bank_holidays,
LATERAL FLATTEN(input => data['england-and-wales']['events']);


CREATE OR REPLACE VIEW view_transformed_hw_data AS
SELECT 
  value:height_in::FLOAT AS height_in,
  value:weight_lb::FLOAT AS weight_lb,
  ROUND(value:weight_lb::FLOAT / POWER(value:height_in::FLOAT, 2) * 703, 2) AS bmi
FROM raw_hw_data,
LATERAL FLATTEN(input => data);


