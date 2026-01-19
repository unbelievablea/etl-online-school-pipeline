-- новый код для заполнения mart.f_sales (включает статус и отрицательные значения суммы для возвратов)
INSERT INTO mart.f_sales (date_id, item_id, customer_id, city_id, quantity, payment_amount, status)
SELECT 
    dc.date_id,
    uol.item_id,
    uol.customer_id,
    uol.city_id,
    uol.quantity,
    CASE 
        WHEN uol.status = 'refunded' THEN -uol.payment_amount
        ELSE uol.payment_amount
    END as payment_amount,
    uol.status
FROM staging.user_order_log uol
LEFT JOIN mart.d_calendar as dc ON uol.date_time::Date = dc.date_actual
WHERE uol.date_time::Date = '{{ds}}';