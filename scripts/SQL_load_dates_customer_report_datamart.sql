-- делаем запись в таблицу загрузок о том, когда была совершена загрузка, чтобы в следующий раз взять данные, которые будут добавлены или изменены после этой даты
INSERT INTO dwh.load_dates_customer_report_datamart (
		load_dttm
	)
	SELECT 
	GREATEST(dd.max_load_crafstman, dd.max_load_customers, dd.max_load_products)
    FROM (
        SELECT 
            COALESCE(MAX(craftsman_load_dttm), NOW()) AS max_load_crafstman,
            COALESCE(MAX(customer_load_dttm), NOW()) AS max_load_customers,
            COALESCE(MAX(products_load_dttm), NOW()) AS max_load_products
		FROM dwh.dwh_delta) AS dd;
		
-- проверка данных
SELECT * FROM dwh.load_dates_customer_report_datamart;