-- Создание временных таблиц для переноса данных из extarnal_source в dwh
BEGIN;
DROP TABLE IF EXISTS tmp_craft_products_orders;

CREATE TEMP TABLE tmp_craft_products_orders AS 
SELECT DISTINCT *
FROM external_source.craft_products_orders;

DROP TABLE IF EXISTS tmp_customers;

CREATE TEMP TABLE tmp_customers AS 
SELECT DISTINCT *
FROM external_source.customers;

-- Добавление данных в таблицу d_customer
MERGE INTO dwh.d_customer AS dc
USING (SELECT customer_name, customer_address, customer_birthday, customer_email FROM tmp_customers) tc 
ON dc.customer_name = tc.customer_name AND dc.customer_email = tc.customer_email
WHEN MATCHED THEN 
UPDATE SET customer_birthday = tc.customer_birthday, customer_address = tc.customer_address, load_dttm = CURRENT_TIMESTAMP 
WHEN NOT MATCHED THEN 
INSERT (customer_name, customer_address, customer_birthday, customer_email, load_dttm)
VALUES (tc.customer_name, tc.customer_address, tc.customer_birthday, tc.customer_email, CURRENT_TIMESTAMP);

-- Добавление данных в таблицу d_craftsman
MERGE INTO dwh.d_craftsman AS dc 
USING (SELECT DISTINCT craftsman_name, craftsman_address, craftsman_birthday, craftsman_email FROM tmp_craft_products_orders) tcp
ON dc.craftsman_name = tcp.craftsman_name AND dc.craftsman_email = tcp.craftsman_email
WHEN MATCHED THEN 
UPDATE SET craftsman_birthday = tcp.craftsman_birthday, craftsman_address = tcp.craftsman_address, load_dttm = CURRENT_TIMESTAMP 
WHEN NOT MATCHED THEN 
INSERT (craftsman_name, craftsman_address, craftsman_birthday, craftsman_email, load_dttm)
VALUES (tcp.craftsman_name, tcp.craftsman_address, tcp.craftsman_birthday, tcp.craftsman_email, CURRENT_TIMESTAMP);

-- Добавление данных в таблицу d_product
MERGE INTO dwh.d_product AS dp 
USING (SELECT DISTINCT product_name, product_description, product_type, product_price FROM tmp_craft_products_orders) tcp
ON dp.product_name = tcp.product_name AND dp.product_description = tcp.product_description AND dp.product_price = tcp.product_price 
WHEN MATCHED THEN 
UPDATE SET product_type = tcp.product_type, load_dttm = CURRENT_TIMESTAMP 
WHEN NOT MATCHED THEN 
INSERT (product_name, product_description, product_type, product_price, load_dttm)
VALUES (tcp.product_name, tcp.product_description, tcp.product_type, tcp.product_price, CURRENT_TIMESTAMP);

-- Добавление данных в таблицу f_order
-- Создаем временную таблицу tmp_order для синхронизации с f_order по столбцам
DROP TABLE IF EXISTS tmp_order;
CREATE TEMP TABLE tmp_order AS 
SELECT DISTINCT
	dp.product_id,
	dc.craftsman_id,
	dcus.customer_id,
	tcp.order_created_date,
	tcp.order_completion_date,
	tcp.order_status,
	CURRENT_TIMESTAMP as load_dttm
FROM tmp_craft_products_orders tcp 
JOIN tmp_customers tc ON tcp.customer_id = tc.customer_id
JOIN dwh.d_customer dcus ON tc.customer_name = dcus.customer_name AND tc.customer_email = dcus.customer_email
JOIN dwh.d_craftsman dc ON tcp.craftsman_name = dc.craftsman_name AND tcp.craftsman_email = dc.craftsman_email  
JOIN dwh.d_product dp ON tcp.product_name = dp.product_name AND tcp.product_description = dp.product_description AND tcp.product_price = dp.product_price;

-- Проверка корректности формирования временной таблицы tmp_order
SELECT COUNT(*)
FROM tmp_order;

MERGE INTO dwh.f_order AS fo 
USING (SELECT DISTINCT * FROM tmp_order) tmp 
ON fo.product_id = tmp.product_id AND fo.craftsman_id = tmp.craftsman_id AND fo.customer_id = tmp.customer_id AND
fo.order_created_date = tmp.order_created_date AND fo.order_completion_date = tmp.order_completion_date AND 
fo.order_status = tmp.order_status
WHEN MATCHED THEN 
UPDATE SET load_dttm = CURRENT_TIMESTAMP 
WHEN NOT MATCHED THEN 
INSERT (product_id, craftsman_id, customer_id, order_created_date, order_completion_date, order_status, load_dttm)
VALUES (tmp.product_id, tmp.craftsman_id, tmp.customer_id, tmp.order_created_date, tmp.order_completion_date, tmp.order_status, CURRENT_TIMESTAMP);

COMMIT;