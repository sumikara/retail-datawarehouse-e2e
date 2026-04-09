-- incremental load için neden tekrar bu tabloyu drop edip tekrar yaratıyorum anlamadım. Bence bu yanlış. Çünkü ilk 475k row yüklediğim 
-- staging tabloları DROP ediyorum ve 25k incremental load yapıyorum. Ancak bu durum mapping(t_map) tablolarımda sl_online_retail.src_online_retail kullanarak yüklenen verileri bozmaz mı?
-- Ben bu işlem yerine ilk bulk load sırasında create select ile tablo kurmak yerine direkt içine aldığı kolonları standardize eden bir tablo yapısı yaratsam ve incremental load sırasında sadece insert into select kullansam daha mantıklı değil mi? bu aşama kafamı karıştırdı.

-- ONLINE CLEAN STAGING

DROP TABLE IF EXISTS sl_online_retail.src_online_retail;

CREATE TABLE sl_online_retail.src_online_retail AS
SELECT
    /* ===================== CUSTOMER ===================== */
    COALESCE(NULLIF(LOWER(TRIM(customer_id)), ''), 'n.a.') AS customer_id,
    COALESCE(NULLIF(LOWER(TRIM(gender)), ''), 'n.a.') AS gender,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(marital_status),' ','_')), ''), 'n.a.') AS marital_status,

    CASE
        WHEN NULLIF(TRIM(date_of_birth), '') IS NOT NULL
        AND TRIM(date_of_birth) ~ '^\d{2}-\d{2}-\d{4}$'
        THEN TO_DATE(TRIM(date_of_birth), 'DD-MM-YYYY')
        WHEN NULLIF(TRIM(date_of_birth), '') IS NOT NULL
        AND TRIM(date_of_birth) ~ '^\d{2}/\d{2}/\d{4}$'
        THEN TO_DATE(TRIM(date_of_birth), 'DD/MM/YYYY')
    END AS birth_of_dt,

    CASE
        WHEN NULLIF(TRIM(membership_date), '') IS NOT NULL
        AND TRIM(membership_date) ~ '^\d{2}-\d{2}-\d{4}$'
        THEN TO_DATE(TRIM(membership_date), 'DD-MM-YYYY')
        WHEN NULLIF(TRIM(membership_date), '') IS NOT NULL
        AND TRIM(membership_date) ~ '^\d{2}/\d{2}/\d{4}$'
        THEN TO_DATE(TRIM(membership_date), 'DD/MM/YYYY')
    END AS membership_dt,

    CASE
        WHEN NULLIF(TRIM(last_purchase_date), '') IS NOT NULL
        AND TRIM(last_purchase_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(last_purchase_date), 'DD-MM-YYYY HH24:MI')
        WHEN NULLIF(TRIM(last_purchase_date), '') IS NOT NULL
        AND TRIM(last_purchase_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(last_purchase_date), 'DD/MM/YYYY HH24:MI')
    END AS last_purchase_dt,

    COALESCE(NULLIF(TRIM(customer_zip_code), ''), 'n.a.') AS customer_zip_code,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(customer_city),' ','_')), ''), 'n.a.') AS customer_city,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(customer_state),' ','_')), ''), 'n.a.') AS customer_state,


    /* ===================== TRANSACTION ===================== */
    COALESCE(NULLIF(LOWER(TRIM(transaction_id)), ''), 'n.a.') AS transaction_id,

    CASE
        WHEN NULLIF(TRIM(transaction_date), '') IS NOT NULL
        AND TRIM(transaction_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(transaction_date), 'DD-MM-YYYY HH24:MI')
        WHEN NULLIF(TRIM(transaction_date), '') IS NOT NULL
        AND TRIM(transaction_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(transaction_date), 'DD/MM/YYYY HH24:MI')
    END AS transaction_dt,

 /* ===================== FINANCIAL ===================== */

    CASE
      WHEN TRIM(quantity) ~ '^-?\d+$'
      THEN quantity::INT
    END  AS quantity,

      CASE
        WHEN TRIM(unit_price) ~ '^-?\d+(\.\d+)?$'
        THEN unit_price::NUMERIC(10,2)
      END AS unit_price,

    CASE
      WHEN TRIM(total_sales) ~ '^-?\d+(\.\d+)?$'
      THEN total_sales::NUMERIC(10,2)
    END AS total_sales,

    CASE
      WHEN TRIM(discount_applied) ~ '^-?\d+(\.\d+)?$'
      THEN discount_applied::NUMERIC(10,2)
    END AS discount_applied,

COALESCE(NULLIF(LOWER(REPLACE(TRIM(payment_method), ' ', '_')), ''), 'n.a.') AS payment_method,

    /* ===================== TIME ===================== */
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(day_of_week), ' ', '_')), ''), 'n.a.') AS day_of_week,

        CASE
            WHEN TRIM(week_of_year) ~ '^\d+$'
            THEN CAST(TRIM(week_of_year) AS INTEGER)
        END AS week_of_year,

        CASE
            WHEN TRIM(month_of_year) ~ '^\d+$'
            THEN CAST(TRIM(month_of_year) AS INTEGER)
        END AS month_of_year,


    /* ===================== PRODUCT ===================== */
    COALESCE(NULLIF(LOWER(TRIM(product_id)), ''), 'n.a.') AS product_id,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(product_category),' ','_')), ''), 'n.a.') AS product_category,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(product_name),' ','_')), ''), 'n.a.') AS product_name,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(product_brand),' ','_')), ''), 'n.a.') AS product_brand,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(product_material),' ','_')), ''), 'n.a.') AS product_material,

    CASE WHEN NULLIF(TRIM(product_stock), '') IS NOT NULL
         THEN product_stock::INT END AS product_stock,

    CASE
        WHEN NULLIF(TRIM(product_manufacture_date), '') IS NOT NULL
        AND TRIM(product_manufacture_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(product_manufacture_date), 'DD-MM-YYYY HH24:MI')
        WHEN NULLIF(TRIM(product_manufacture_date), '') IS NOT NULL
        AND TRIM(product_manufacture_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(product_manufacture_date), 'DD/MM/YYYY HH24:MI')
    END AS product_manufacture_dt,

    CASE
        WHEN NULLIF(TRIM(product_expiry_date), '') IS NOT NULL
        AND TRIM(product_expiry_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(product_expiry_date), 'DD-MM-YYYY HH24:MI')
        WHEN NULLIF(TRIM(product_expiry_date), '') IS NOT NULL
        AND TRIM(product_expiry_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(product_expiry_date), 'DD/MM/YYYY HH24:MI')
    END AS product_expiry_dt,

    /* ===================== PROMOTION ===================== */
    COALESCE(NULLIF(LOWER(TRIM(promotion_id)), ''), 'n.a.') AS promotion_id,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(promotion_type),' ','_')), ''), 'n.a.') AS promotion_type,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(promotion_channel),' ','_')), ''), 'n.a.') AS promotion_channel,

 CASE
    WHEN NULLIF(TRIM(promotion_start_date), '') IS NOT NULL
     AND TRIM(promotion_start_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
    THEN TO_TIMESTAMP(TRIM(promotion_start_date), 'DD-MM-YYYY HH24:MI')
    WHEN NULLIF(TRIM(promotion_start_date), '') IS NOT NULL
     AND TRIM(promotion_start_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
    THEN TO_TIMESTAMP(TRIM(promotion_start_date), 'DD/MM/YYYY HH24:MI')
END AS promotion_start_dt,

CASE
    WHEN NULLIF(TRIM(promotion_end_date), '') IS NOT NULL
     AND TRIM(promotion_end_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
    THEN TO_TIMESTAMP(TRIM(promotion_end_date), 'DD-MM-YYYY HH24:MI')
    WHEN NULLIF(TRIM(promotion_end_date), '') IS NOT NULL
     AND TRIM(promotion_end_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
    THEN TO_TIMESTAMP(TRIM(promotion_end_date), 'DD/MM/YYYY HH24:MI')
END AS promotion_end_dt,


    /* ===================== DELIVERY ===================== */
    COALESCE(NULLIF(LOWER(TRIM(delivery_id)), ''), 'n.a.') AS delivery_id,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(delivery_type), ' ', '_')), ''), 'n.a.') AS delivery_type,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(delivery_status), ' ', '_')), ''), 'n.a.') AS delivery_status,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(shipping_partner),' ','_')), ''), 'n.a.') AS shipping_partner,

   /* ===================== ENGAGEMENT ===================== */

    COALESCE(NULLIF(LOWER(TRIM(engagement_id)), ''), 'n.a.') AS engagement_id,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(website_address), ' ', '_')), ''), 'n.a.') AS website_address,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(order_channel), ' ', '_')), ''), 'n.a.') AS order_channel,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(customer_support_method), ' ', '_')), ''), 'n.a.') AS customer_support_method,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(issue_status), ' ', '_')), ''), 'n.a.') AS issue_status,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(app_usage), ' ', '_')), ''), 'n.a.') AS app_usage,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(social_media_engagement), ' ', '_')), ''), 'n.a.') AS social_media_engagement,

    /* SAFE INTEGER CAST */
    CASE
        WHEN TRIM(src.website_visits) ~ '^\d+$'
        THEN CAST(TRIM(src.website_visits) AS INTEGER)
    END AS website_visits,

    CASE
        WHEN TRIM(src.customer_support_calls) ~ '^\d+$'
        THEN CAST(TRIM(src.customer_support_calls) AS INTEGER)
    END AS customer_support_calls,

        /* ===================== META ===================== */
        insert_dt

    FROM sl_online_retail.src_online_retail_raw src;


/* =========================================================
   OFFLINE CLEAN STAGING
   ========================================================= */

DROP TABLE IF EXISTS sl_offline_retail.src_offline_retail;

CREATE TABLE sl_offline_retail.src_offline_retail AS
SELECT
    /* ===================== CUSTOMER ===================== */
    COALESCE(NULLIF(LOWER(TRIM(customer_id)), ''), 'n.a.') AS customer_id,
    COALESCE(NULLIF(LOWER(TRIM(gender)), ''), 'n.a.') AS gender,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(marital_status),' ','_')), ''), 'n.a.') AS marital_status,

    CASE
        WHEN NULLIF(TRIM(date_of_birth), '') IS NOT NULL
        AND TRIM(date_of_birth) ~ '^\d{2}-\d{2}-\d{4}$'
        THEN TO_DATE(TRIM(date_of_birth), 'DD-MM-YYYY')
        WHEN NULLIF(TRIM(date_of_birth), '') IS NOT NULL
        AND TRIM(date_of_birth) ~ '^\d{2}/\d{2}/\d{4}$'
        THEN TO_DATE(TRIM(date_of_birth), 'DD/MM/YYYY')
    END AS birth_of_dt,

    CASE
        WHEN NULLIF(TRIM(membership_date), '') IS NOT NULL
        AND TRIM(membership_date) ~ '^\d{2}-\d{2}-\d{4}$'
        THEN TO_DATE(TRIM(membership_date), 'DD-MM-YYYY')
        WHEN NULLIF(TRIM(membership_date), '') IS NOT NULL
        AND TRIM(membership_date) ~ '^\d{2}/\d{2}/\d{4}$'
        THEN TO_DATE(TRIM(membership_date), 'DD/MM/YYYY')
    END AS membership_dt,

    CASE
        WHEN NULLIF(TRIM(last_purchase_date), '') IS NOT NULL
        AND TRIM(last_purchase_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(last_purchase_date), 'DD-MM-YYYY HH24:MI')
        WHEN NULLIF(TRIM(last_purchase_date), '') IS NOT NULL
        AND TRIM(last_purchase_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(last_purchase_date), 'DD/MM/YYYY HH24:MI')
    END AS last_purchase_dt,

    COALESCE(NULLIF(TRIM(customer_zip_code), ''), 'n.a.') AS customer_zip_code,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(customer_city),' ','_')), ''), 'n.a.') AS customer_city,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(customer_state),' ','_')), ''), 'n.a.') AS customer_state,


    /* ===================== TRANSACTION ===================== */
    COALESCE(NULLIF(LOWER(TRIM(transaction_id)), ''), 'n.a.') AS transaction_id,

    CASE
        WHEN NULLIF(TRIM(transaction_date), '') IS NOT NULL
        AND TRIM(transaction_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(transaction_date), 'DD-MM-YYYY HH24:MI')
        WHEN NULLIF(TRIM(transaction_date), '') IS NOT NULL
        AND TRIM(transaction_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(transaction_date), 'DD/MM/YYYY HH24:MI')
    END AS transaction_dt,

/* ===================== FINANCIAL ===================== */

    CASE
      WHEN TRIM(quantity) ~ '^-?\d+$'
      THEN quantity::INT
    END  AS quantity,

      CASE
        WHEN TRIM(unit_price) ~ '^-?\d+(\.\d+)?$'
        THEN unit_price::NUMERIC(10,2)
      END AS unit_price,

    CASE
      WHEN TRIM(total_sales) ~ '^-?\d+(\.\d+)?$'
      THEN total_sales::NUMERIC(10,2)
    END AS total_sales,

    CASE
      WHEN TRIM(discount_applied) ~ '^-?\d+(\.\d+)?$'
      THEN discount_applied::NUMERIC(10,2)
    END AS discount_applied,

COALESCE(NULLIF(LOWER(REPLACE(TRIM(payment_method), ' ', '_')), ''), 'n.a.') AS payment_method,

   /* ===================== TIME ===================== */
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(day_of_week), ' ', '_')), ''), 'n.a.') AS day_of_week,

   CASE
    WHEN TRIM(week_of_year) ~ '^\d+$'
    THEN CAST(TRIM(week_of_year) AS INTEGER)
END AS week_of_year,

CASE
    WHEN TRIM(month_of_year) ~ '^\d+$'
    THEN CAST(TRIM(month_of_year) AS INTEGER)
END AS month_of_year,


    /* ===================== STORE ===================== */
    COALESCE(NULLIF(TRIM(store_zip_code), ''), 'n.a.') AS store_zip_code,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(store_city),' ','_')), ''), 'n.a.') AS store_city,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(store_state),' ','_')), ''), 'n.a.') AS store_state,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(store_location),' ','_')), ''), 'n.a.') AS store_location,

    /* ===================== EMPLOYEE ===================== */
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(employee_name),' ','_')), ''), 'n.a.') AS employee_name,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(employee_position),' ','_')), ''), 'n.a.') AS employee_position,

        CASE
            WHEN TRIM(employee_salary) ~ '^-?\d+(\.\d+)?$'
            THEN CAST(TRIM(employee_salary) AS NUMERIC(10,2))
        END AS employee_salary,
          CASE
              WHEN NULLIF(TRIM(employee_hire_date), '') IS NOT NULL
              AND TRIM(employee_hire_date) ~ '^\d{2}-\d{2}-\d{4}$'
              THEN TO_DATE(TRIM(employee_hire_date), 'DD-MM-YYYY')
              WHEN NULLIF(TRIM(employee_hire_date), '') IS NOT NULL
              AND TRIM(employee_hire_date) ~ '^\d{2}/\d{2}/\d{4}$'
              THEN TO_DATE(TRIM(employee_hire_date), 'DD/MM/YYYY')
          END AS employee_hire_date,

    /* ===================== PRODUCT ===================== */
    COALESCE(NULLIF(TRIM(product_id), ''), 'n.a.') AS product_id,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(product_category),' ','_')), ''), 'n.a.') AS product_category,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(product_name),' ','_')), ''), 'n.a.') AS product_name,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(product_brand),' ','_')), ''), 'n.a.') AS product_brand,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(product_material),' ','_')), ''), 'n.a.') AS product_material,

        CASE
            WHEN TRIM(product_stock) ~ '^\d+$'
            THEN CAST(TRIM(product_stock) AS INTEGER)
        END AS product_stock,
    CASE
        WHEN NULLIF(TRIM(product_manufacture_date), '') IS NOT NULL
        AND TRIM(product_manufacture_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(product_manufacture_date), 'DD-MM-YYYY HH24:MI')
        WHEN NULLIF(TRIM(product_manufacture_date), '') IS NOT NULL
        AND TRIM(product_manufacture_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(product_manufacture_date), 'DD/MM/YYYY HH24:MI')
    END AS product_manufacture_dt,

    CASE
        WHEN NULLIF(TRIM(product_expiry_date), '') IS NOT NULL
        AND TRIM(product_expiry_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(product_expiry_date), 'DD-MM-YYYY HH24:MI')
        WHEN NULLIF(TRIM(product_expiry_date), '') IS NOT NULL
        AND TRIM(product_expiry_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
        THEN TO_TIMESTAMP(TRIM(product_expiry_date), 'DD/MM/YYYY HH24:MI')
    END AS product_expiry_dt,

    /* ===================== PROMOTION ===================== */
    COALESCE(NULLIF(TRIM(promotion_id), ''), 'n.a.') AS promotion_id,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(promotion_type),' ','_')), ''), 'n.a.') AS promotion_type,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(promotion_channel),' ','_')), ''), 'n.a.') AS promotion_channel,

 CASE
    WHEN NULLIF(TRIM(promotion_start_date), '') IS NOT NULL
     AND TRIM(promotion_start_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
    THEN TO_TIMESTAMP(TRIM(promotion_start_date), 'DD-MM-YYYY HH24:MI')
    WHEN NULLIF(TRIM(promotion_start_date), '') IS NOT NULL
     AND TRIM(promotion_start_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
    THEN TO_TIMESTAMP(TRIM(promotion_start_date), 'DD/MM/YYYY HH24:MI')
END AS promotion_start_dt,

CASE
    WHEN NULLIF(TRIM(promotion_end_date), '') IS NOT NULL
     AND TRIM(promotion_end_date) ~ '^\d{2}-\d{2}-\d{4} \d{2}:\d{2}$'
    THEN TO_TIMESTAMP(TRIM(promotion_end_date), 'DD-MM-YYYY HH24:MI')
    WHEN NULLIF(TRIM(promotion_end_date), '') IS NOT NULL
     AND TRIM(promotion_end_date) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$'
    THEN TO_TIMESTAMP(TRIM(promotion_end_date), 'DD/MM/YYYY HH24:MI')
END AS promotion_end_dt,

 /* ===================== DELIVERY ===================== */
    COALESCE(NULLIF(TRIM(delivery_id), ''), 'n.a.') AS delivery_id,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(delivery_type), ' ', '_')), ''), 'n.a.') AS delivery_type,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(delivery_status), ' ', '_')), ''), 'n.a.') AS delivery_status,
    COALESCE(NULLIF(LOWER(REPLACE(TRIM(shipping_partner),' ','_')), ''), 'n.a.') AS shipping_partner,


    /* ===================== META ===================== */
    insert_dt

FROM sl_offline_retail.src_offline_retail_raw;
