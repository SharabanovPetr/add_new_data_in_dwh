BEGIN;

-- создаём таблицу dwh.dwh_delta_insert_result: делаем расчёт витрины по новым данным, которых нет, для вставки в витрину
DROP TABLE IF EXISTS dwh.dwh_delta_insert_result;

CREATE TABLE IF NOT EXISTS dwh.dwh_delta_insert_result AS ( 
	SELECT  
		T5.customer_id AS customer_id,
		T5.customer_name AS customer_name,
		T5.customer_address AS customer_address,
		T5.customer_birthday AS customer_birthday,
		T5.customer_email AS customer_email,
		T5.customer_money AS customer_money,
		T5.platform_money AS platform_money,
		T5.count_order AS count_order,
		T5.avg_price_order AS avg_price_order,
		T5.median_time_order_completed AS median_time_order_completed,
		T5.product_type AS top_product_type,
		T5.craftsman_id AS top_craftsman_id,
		T5.count_order_created AS count_order_created,
		T5.count_order_in_progress AS count_order_in_progress,
		T5.count_order_delivery AS count_order_delivery,
		T5.count_order_done AS count_order_done,
		T5.count_order_not_done AS count_order_not_done,
		T5.report_period AS report_period 
	FROM (
		SELECT 
			*,
			RANK() OVER(PARTITION BY T2.customer_id ORDER BY T3.count_product DESC) AS rank_count_product,
			ROW_NUMBER() OVER(PARTITION BY T2.customer_id ORDER BY T4.count_order_for_customer DESC) AS rank_top_craftsman
		FROM ( 
			SELECT
				customer_id,
				customer_name,
				customer_address,
				customer_birthday,
				customer_email,
				report_period,
				SUM(product_price) AS customer_money,
				0.1 * SUM(product_price) AS platform_money,
				COUNT(order_id) AS count_order,
				AVG(product_price) AS avg_price_order,
				percentile_cont(0.5) WITHIN GROUP(ORDER BY diff_order_date) AS median_time_order_completed,
                SUM(CASE WHEN order_status = 'created' THEN 1 ELSE 0 END) AS count_order_created,
                SUM(CASE WHEN order_status = 'in progress' THEN 1 ELSE 0 END) AS count_order_in_progress,
                SUM(CASE WHEN order_status = 'delivery' THEN 1 ELSE 0 END) AS count_order_delivery,
                SUM(CASE WHEN order_status = 'done' THEN 1 ELSE 0 END) AS count_order_done,
                SUM(CASE WHEN order_status != 'done' THEN 1 ELSE 0 END) AS count_order_not_done
			FROM dwh.dwh_delta AS T1
			WHERE T1.exist_customer_id IS NULL
			GROUP BY T1.customer_id, T1.customer_name, T1.customer_address, T1.customer_birthday, T1.customer_email, T1.report_period
				) AS T2 
		-- Формируем выборку для определения top_product_type в оконной функции
		INNER JOIN (
				SELECT
					customer_id AS customer_id_for_product_type,
					product_type,
					COUNT(product_type) AS count_product
				FROM dwh.dwh_delta AS dd
				GROUP BY dd.customer_id, dd.product_type
				ORDER BY count_product DESC
					) AS T3 
					ON T2.customer_id = T3.customer_id_for_product_type
		-- Формируем выборку для определения top_craftsman_id в оконной функции
		INNER JOIN (
				SELECT
					customer_id AS customer_id_for_craftsman_id,
					craftsman_id,
					COUNT(craftsman_id) AS count_order_for_customer
				FROM dwh.dwh_delta AS dd
				GROUP BY dd.customer_id, dd.craftsman_id
				ORDER BY count_order_for_customer DESC
					) AS T4 
					ON T2.customer_id = T4.customer_id_for_craftsman_id
			) AS T5
		WHERE T5.rank_count_product = 1 AND T5.rank_top_craftsman = 1 ORDER BY report_period
);

-- проверка данных
SELECT * FROM dwh.dwh_delta_insert_result;

COMMIT;