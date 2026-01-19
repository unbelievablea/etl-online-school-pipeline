-- создаем таблицу если её нет
CREATE TABLE IF NOT EXISTS mart.f_customer_retention (
    new_customers_count int NOT NULL DEFAULT 0,
    returning_customers_count int NOT NULL DEFAULT 0,
    refunded_customer_count int NOT NULL DEFAULT 0,
    period_name varchar(10) NOT NULL DEFAULT 'weekly',
    period_id int NOT NULL,
    item_id int NOT NULL,
    new_customers_revenue numeric(10, 2) NOT NULL DEFAULT 0,
    returning_customers_revenue numeric(10, 2) NOT NULL DEFAULT 0,
    customers_refunded int NOT NULL DEFAULT 0,
    PRIMARY KEY (period_id, item_id)
);

-- удаляем старые данные и вставляем новые
DELETE FROM mart.f_customer_retention;

INSERT INTO mart.f_customer_retention (
    period_id,
    period_name,
    item_id,
    new_customers_count,
    returning_customers_count,
    refunded_customer_count,
    new_customers_revenue,
    returning_customers_revenue,
    customers_refunded
)

WITH weeks_agg AS (
    SELECT 
        EXTRACT(WEEK FROM dc.date_actual) as week_number,
        fs.item_id,
        fs.customer_id,
        COUNT(*) as orders_per_week,
        MAX(CASE WHEN fs.status = 'refunded' THEN 1 ELSE 0 END) as had_refund,
        SUM(CASE WHEN fs.status = 'refunded' THEN 1 ELSE 0 END) as refunds_count,
        SUM(fs.payment_amount) as total_amount
    FROM mart.f_sales fs
    JOIN mart.d_calendar dc ON fs.date_id = dc.date_id
    GROUP BY EXTRACT(WEEK FROM dc.date_actual), fs.item_id, fs.customer_id
)

SELECT 
    week_number,
    'weekly',
    item_id,
    COUNT(CASE WHEN orders_per_week = 1 THEN customer_id END),
    COUNT(CASE WHEN orders_per_week > 1 THEN customer_id END),
    COUNT(CASE WHEN had_refund = 1 THEN customer_id END),
    SUM(CASE WHEN orders_per_week = 1 THEN total_amount ELSE 0 END),
    SUM(CASE WHEN orders_per_week > 1 THEN total_amount ELSE 0 END),
    SUM(refunds_count)
FROM weeks_agg
GROUP BY week_number, item_id;