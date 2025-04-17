--> Virtual Warehouses Code through SQL
--> It can also be performed through Snowsight (Snowflake Web Based UI)

---> what menu items does the Freezing Point brand sell?
SELECT 
   menu_item_name
FROM tasty_bytes_sample_data.raw_pos.menu
WHERE truck_brand_name = 'Freezing Point';

---> what is the profit on Mango Sticky Rice?
SELECT 
   menu_item_name,
   (sale_price_usd - cost_of_goods_usd) AS profit_usd
FROM tasty_bytes_sample_data.raw_pos.menu
WHERE 1=1
AND truck_brand_name = 'Freezing Point'
AND menu_item_name = 'Mango Sticky Rice';


--> Warehouse Creation -- Default to X-Small
CREATE WAREHOUSE warehouse_dash;
CREATE WAREHOUSE warehouse_gilberto;

--> List the Warehouses 
SHOW WAREHOUSES;

--> Use the warehouse
USE WAREHOUSE warehouse_gilberto;

---> Verticle Scaling - Changing the Size of the Warehouse
---> set warehouse size to medium
ALTER WAREHOUSE warehouse_dash SET warehouse_size=MEDIUM;

USE WAREHOUSE warehouse_dash;

SELECT
    menu_item_name,
   (sale_price_usd - cost_of_goods_usd) AS profit_usd
FROM tasty_bytes_sample_data.raw_pos.menu
ORDER BY 2 DESC;

---> set warehouse size to xsmall - altering 
ALTER WAREHOUSE warehouse_dash SET warehouse_size=XSMALL;

---> drop warehouse
DROP WAREHOUSE warehouse_vino;

SHOW WAREHOUSES;

---> Horizontal Scaling - Increasing the cluster account from 1 to 3 --> By Default it is 1

---> create a multi-cluster warehouse (max clusters = 3)
CREATE WAREHOUSE warehouse_vino MAX_CLUSTER_COUNT = 3;

SHOW WAREHOUSES;

---> set the auto_suspend and auto_resume parameters
ALTER WAREHOUSE warehouse_dash SET AUTO_SUSPEND = 180 AUTO_RESUME = FALSE;

SHOW WAREHOUSES;
