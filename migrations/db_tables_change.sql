-- скрипты для разового запуска перед новым DAG, корректируем исходные таблицы
ALTER TABLE staging.user_order_log ADD COLUMN status VARCHAR(20);
UPDATE staging.user_order_log SET status = 'shipped';

ALTER TABLE mart.f_sales ADD COLUMN status VARCHAR(20);
UPDATE mart.f_sales SET status = 'shipped';