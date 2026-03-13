BEGIN;

-- dwh_delta: определяем, какие данные были изменены или добавлены в DWH 
DROP TABLE IF EXISTS dwh.dwh_delta;

CREATE TABLE IF NOT EXISTS dwh.dwh_delta AS (
	SELECT 	
		dcs.customer_id,
		dcs.customer_name,
		dcs.customer_address,
		dcs.customer_birthday,
		dcs.customer_email,
		fo.order_id,
		dp.product_id,
		dp.product_price,
		dp.product_type,
		fo.order_completion_date - fo.order_created_date AS diff_order_date, 
		fo.order_status,
		dc.craftsman_id,
		to_char(fo.order_created_date, 'yyyy-mm') AS report_period,
		crd.id AS exist_customer_id,
		dc.load_dttm AS craftsman_load_dttm,
		dcs.load_dttm AS customer_load_dttm,
		dp.load_dttm AS products_load_dttm	
	FROM dwh.f_order fo 
	INNER JOIN dwh.d_craftsman dc ON fo.craftsman_id = dc.craftsman_id
	INNER JOIN dwh.d_customer dcs ON fo.customer_id = dcs.customer_id
	INNER JOIN dwh.d_product dp ON fo.product_id = dp.product_id
	LEFT JOIN dwh.customer_report_datamart crd ON fo.customer_id = crd.customer_id
	WHERE 
        (fo.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
        (dc.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
		(dcs.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart)) OR
		(dp.load_dttm > (SELECT COALESCE(MAX(load_dttm), '1900-01-01') FROM dwh.load_dates_customer_report_datamart))
);

-- создаём таблицу dwh.dwh_update_delta: делаем выборку заказчиков, по которым были изменения в DWH.
DROP TABLE IF EXISTS dwh.dwh_update_delta;

CREATE TABLE IF NOT EXISTS dwh.dwh_update_delta AS (
	SELECT 	
		customer_id
	FROM dwh.dwh_delta dd 
	WHERE dd.exist_customer_id IS NOT NULL	
);

COMMIT;