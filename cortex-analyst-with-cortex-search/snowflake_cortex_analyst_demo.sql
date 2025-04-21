--> Let's create Database, Schema, Warehouse and Stage Creation

/*
Use this, if you're not working in Account Admin Role
*/

use role securityadmin;

--> Using the Security Admin create a Cortex User Role
create role cortex_user_role;
grant database role snowflake.cortex to role cortex_user_role;

--> Assign the role to the user
grant role cortex_user_role to user <user-name>;

--> Create a database
create or replace database cortex_analyst_demo;
--> Create a schema
create or replace schema cortex_analyst_demo.revenue_timeseries;

--> Create a warehouse
--> Note you can use the same COMPUTE_WH (x-small) warehouse
create or replace warehouse cortex_analyst_wh
warehouse_size = 'large'
warehouse_type = 'standard'
auto_suspend = 60
auto_resume = true
initially_suspended = true
comment = 'Warehouse for Cortex Analyst Demo';

--> Access Privileges
grant usage on warehouse cortex_analyst_wh to role cortex_user_role;
grant operate on warehouse cortex_analyst_wh to role cortex_user_role;

grant ownership on schema cortex_analyst_demo.revenue_timeseries to role cortex_user_role;
grant ownership on database cortex_analyst_demo to role cortex_user_role;

--> Use the features
use role cortex_use_role;
use warehouse cortex_analyst_wh;
use database cortex_analyst_demo;
use schema cortex_analyst_demo.revenue_timeseries;

--> Create a stage for raw data
create or replace stage raw_data DIRECTORY = (ENABLE = TRUE);


-- Fact table: daily_revenue
create or replace table cortex_analyst_demo.revenue_timeseries.daily_revenue (
    date date,
    revenue float,
    cogs float,
    forecasted_revenue float,
    product_id int,
    region_id int
);

-- Dimension Table: product_dim
create or replace table cortex_analyst_demo.revenue_timeseries.product_dim(
    product_id int,
    product_line varchar(16777216)
);

-- Dimension Table: region_dim
create or replace table cortex_analyst_demo.revenue_timeseries.region_dim (
    region_id int,
    sales_region varchar(16777216),
    state varchar(16777216)
);


/*
load data into the tables from raw data stage
*/

ls @raw_data;

COPY INTO CORTEX_ANALYST_DEMO.REVENUE_TIMESERIES.DAILY_REVENUE
FROM @raw_data
FILES = ('daily_revenue.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

COPY INTO CORTEX_ANALYST_DEMO.REVENUE_TIMESERIES.PRODUCT_DIM
FROM @raw_data
FILES = ('product.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;



COPY INTO CORTEX_ANALYST_DEMO.REVENUE_TIMESERIES.REGION_DIM
FROM @raw_data
FILES = ('region.csv')
FILE_FORMAT = (
    TYPE=CSV,
    SKIP_HEADER=1,
    FIELD_DELIMITER=',',
    TRIM_SPACE=FALSE,
    FIELD_OPTIONALLY_ENCLOSED_BY=NONE,
    REPLACE_INVALID_CHARACTERS=TRUE,
    DATE_FORMAT=AUTO,
    TIME_FORMAT=AUTO,
    TIMESTAMP_FORMAT=AUTO
    EMPTY_FIELD_AS_NULL = FALSE
    error_on_column_count_mismatch=false
)

ON_ERROR=CONTINUE
FORCE = TRUE ;

--> Let's verify
select * from cortex_analyst_demo.revenue_timeseries.daily_revenue;
select * from cortex_analyst_demo.revenue_timeseries.product_dim;
select * from cortex_analyst_demo.revenue_timeseries.region_dim;


--> Cortex Search Service -- work for product dimension
create or replace cortex search service product_line_search_service
on product_dimension
warehouse = compute_wh
target_lag = '1 hour'
as (
select distinct product_line as product_dimension from product_dim
);