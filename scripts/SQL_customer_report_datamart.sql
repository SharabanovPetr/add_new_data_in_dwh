BEGIN;
-- вставка новых данных в витрину
INSERT INTO dwh.customer_report_datamart (
	customer_id,
    customer_name,
    customer_address,
    customer_birthday,
    customer_email,
    customer_money,
    platform_money,
    count_order,
    avg_price_order,
    median_time_order_completed,
    top_product_category,
    top_craftsman_id,
    count_order_created,
    count_order_in_progress,
    count_order_delivery,
    count_order_done,
    count_order_not_done,
    report_period   
)
SELECT
    customer_id,
    customer_name,
    customer_address,
    customer_birthday,
    customer_email,
    customer_money,
    platform_money,
    count_order,
    avg_price_order,
    median_time_order_completed,
    top_product_type,
    top_craftsman_id,
    count_order_created,
    count_order_in_progress,
    count_order_delivery,
    count_order_done,
    count_order_not_done,
    report_period
FROM dwh.dwh_delta_insert_result;

-- выполняем обновление показателей в отчёте по существующим заказчикам
UPDATE dwh.customer_report_datamart AS crd SET
		customer_id = ddu.customer_id,
        customer_name = ddu.customer_name,
        customer_address = ddu.customer_address,
        customer_birthday = ddu.customer_birthday,
        customer_email = ddu.customer_email,
        customer_money = ddu.customer_money,
        platform_money = ddu.platform_money,
        count_order = ddu.count_order,
        avg_price_order = ddu.avg_price_order,
        median_time_order_completed = ddu.median_time_order_completed,
        top_product_category = ddu.top_product_type,
        top_craftsman_id = ddu.top_craftsman_id,
        count_order_created = ddu.count_order_created,
        count_order_in_progress = ddu.count_order_in_progress,
        count_order_delivery = ddu.count_order_delivery,
        count_order_done = ddu.count_order_done,
        count_order_not_done = ddu.count_order_not_done,
        report_period = ddu.report_period
	FROM (
		SELECT 
			customer_id,
		    customer_name,
		    customer_address,
		    customer_birthday,
		    customer_email,
		    customer_money,
		    platform_money,
		    count_order,
		    avg_price_order,
		    median_time_order_completed,
		    top_product_type,
		    top_craftsman_id,
		    count_order_created,
		    count_order_in_progress,
		    count_order_delivery,
		    count_order_done,
		    count_order_not_done,
		    report_period
            FROM dwh.dwh_delta_update_result) AS ddu
	WHERE crd.customer_id = ddu.customer_id;

COMMIT;