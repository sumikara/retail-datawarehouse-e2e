
CREATE TABLE IF NOT EXISTS 3nf.nf_transactions (
    transaction_id      VARCHAR(100)  NOT NULL,
    transaction_dt      TIMESTAMP     NOT NULL,
    total_sales         NUMERIC(10,2) NOT NULL DEFAULT 0,
    payment_method      VARCHAR(100)  NOT NULL DEFAULT 'n.a.',
    quantity            INTEGER       NOT NULL DEFAULT 0,
    unit_price          NUMERIC(10,2) NOT NULL DEFAULT 0,
    discount_applied    NUMERIC(10,2) NOT NULL DEFAULT 0,
    day_of_week         VARCHAR(20)   NOT NULL DEFAULT 'n.a.',
    week_of_year        INTEGER       NOT NULL DEFAULT -1,
    month_of_year       INTEGER       NOT NULL DEFAULT -1,
    store_id            BIGINT        NOT NULL DEFAULT -1,
    customer_id         BIGINT        NOT NULL DEFAULT -1,
    promotion_id        BIGINT        NOT NULL DEFAULT -1,
    delivery_id         BIGINT        NOT NULL DEFAULT -1,
    product_id          BIGINT        NOT NULL DEFAULT -1,
    engagement_id       BIGINT        NOT NULL DEFAULT -1,
    city_id             BIGINT        NOT NULL DEFAULT -1,
    employee_id         BIGINT        NOT NULL DEFAULT -1,
    row_sig             TEXT          NOT NULL,
    source_system       VARCHAR(100)  NOT NULL,
    source_table        VARCHAR(100)  NOT NULL,
    insert_dt           TIMESTAMP     NOT NULL DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_ce_transactions_store
        FOREIGN KEY (store_id) REFERENCES 3nf.nf_stores(store_id),

    CONSTRAINT fk_ce_transactions_customer
        FOREIGN KEY (customer_id) REFERENCES 3nf.nf_customers(customer_id),

    CONSTRAINT fk_ce_transactions_promotion
        FOREIGN KEY (promotion_id) REFERENCES 3nf.nf_promotions(promotion_id),

    CONSTRAINT fk_ce_transactions_delivery
        FOREIGN KEY (delivery_id) REFERENCES 3nf.nf_deliveries(delivery_id),

    CONSTRAINT fk_ce_transactions_product
        FOREIGN KEY (product_id) REFERENCES 3nf.nf_products(product_id),

    CONSTRAINT fk_ce_transactions_engagement
        FOREIGN KEY (engagement_id) REFERENCES 3nf.nf_engagements(engagement_id),

    CONSTRAINT fk_ce_transactions_city
        FOREIGN KEY (city_id) REFERENCES 3nf.nf_cities(city_id)
);

CREATE INDEX IF NOT EXISTS ix_ce_transactions_trx_id
    ON 3nf.nf_transactions (transaction_id);

CREATE INDEX IF NOT EXISTS ix_ce_transactions_trx_dt_brin
    ON 3nf.nf_transactions USING brin (transaction_dt);

CREATE UNIQUE INDEX IF NOT EXISTS ux_ce_transactions_row_sig
    ON 3nf.nf_transactions (row_sig);

CREATE INDEX IF NOT EXISTS ix_ce_transactions_source
    ON 3nf.nf_transactions (source_system, source_table);

CREATE INDEX IF NOT EXISTS ix_ce_transactions_customer
    ON 3nf.nf_transactions (customer_id);

CREATE INDEX IF NOT EXISTS ix_ce_transactions_product
    ON 3nf.nf_transactions (product_id);

CREATE INDEX IF NOT EXISTS ix_ce_transactions_employee
    ON 3nf.nf_transactions (employee_id);

CREATE INDEX IF NOT EXISTS ix_ce_customers_src_id
    ON 3nf.nf_customers (customer_src_id);

CREATE INDEX IF NOT EXISTS ix_ce_products_src_id
    ON 3nf.nf_products (product_src_id);

CREATE INDEX IF NOT EXISTS ix_ce_promotions_src_id
    ON 3nf.nf_promotions (promotion_src_id);

CREATE INDEX IF NOT EXISTS ix_ce_engagements_src_id
    ON 3nf.nf_engagements (engagement_src_id);

CREATE INDEX IF NOT EXISTS ix_ce_cities_src_id
    ON 3nf.nf_cities (city_src_id);

CREATE INDEX IF NOT EXISTS ix_ce_stores_src_id
    ON 3nf.nf_stores (store_src_id);

CREATE INDEX IF NOT EXISTS ix_ce_emp_lookup
    ON 3nf.nf_employees_scd (employee_src_id, is_active, start_dt, end_dt);

INSERT INTO 3nf.nf_transactions (
    transaction_id,
    transaction_dt,
    total_sales,
    payment_method,
    quantity,
    unit_price,
    discount_applied,
    day_of_week,
    week_of_year,
    month_of_year,
    store_id,
    customer_id,
    promotion_id,
    delivery_id,
    product_id,
    engagement_id,
    city_id,
    employee_id,
    row_sig,
    source_system,
    source_table
)
SELECT
    'n.a.',
    TIMESTAMP '1900-01-01 00:00:00',
    0,
    'n.a.',
    0,
    0,
    0,
    'n.a.',
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    -1,
    'n.a.',
    'MANUAL',
    'MANUAL'
WHERE NOT EXISTS (
    SELECT 1
    FROM 3nf.nf_transactions
    WHERE row_sig = 'n.a.'
);


CREATE OR REPLACE PROCEDURE stg.load_ce_transactions()
LANGUAGE plpgsql
AS $$
DECLARE
    v_proc        TEXT := 'bl_cl.load_ce_transactions';
    v_ins_total   INT  := 0;
    v_err_msg     TEXT;
    v_err_detail  TEXT;
    v_err_hint    TEXT;
BEGIN

    INSERT INTO 3nf.nf_transactions (
        transaction_id,
        transaction_dt,
        total_sales,
        payment_method,
        quantity,
        unit_price,
        discount_applied,
        day_of_week,
        week_of_year,
        month_of_year,
        store_id,
        customer_id,
        promotion_id,
        delivery_id,
        product_id,
        engagement_id,
        city_id,
        employee_id,
        row_sig,
        source_system,
        source_table,
        insert_dt
    )
    SELECT
        t.transaction_id,
        t.transaction_dt,
        COALESCE(t.total_sales, 0),
        COALESCE(t.payment_method, 'n.a.'),
        COALESCE(t.quantity, 0),
        COALESCE(t.unit_price, 0),
        COALESCE(t.discount_applied, 0),
        COALESCE(t.day_of_week, 'n.a.'),
        COALESCE(t.week_of_year, -1),
        COALESCE(t.month_of_year, -1),

        COALESCE(st.store_id, -1)         AS store_id,
        COALESCE(cu.customer_id, -1)      AS customer_id,
        COALESCE(pm.promotion_id, -1)     AS promotion_id,
        COALESCE(dv.delivery_id, -1)      AS delivery_id,
        COALESCE(pr.product_id, -1)       AS product_id,
        COALESCE(eg.engagement_id, -1)    AS engagement_id,
        COALESCE(ci.city_id, -1)          AS city_id,
        COALESCE(emp.employee_id, -1)     AS employee_id,

        t.row_sig,
        t.source_system,
        t.source_table,
        NOW()

    FROM stg.mapping_transactions t

    LEFT JOIN 3nf.nf_customers cu
        ON cu.customer_src_id = t.customer_src_id

    /* product: src_id to src_id */
    LEFT JOIN 3nf.nf_products pr
        ON pr.product_src_id = t.product_src_id

    /* promotion: src_id to src_id */
    LEFT JOIN 3nf.nf_promotions pm
        ON pm.promotion_src_id = t.promotion_src_id

    /* delivery: raw id to natural key */
    LEFT JOIN 3nf.nf_deliveries dv
        ON dv.delivery_src_id = t.delivery_id

    /* engagement: raw id to raw/source key */
    LEFT JOIN 3nf.nf_engagements eg
        ON eg.engagement_src_id = t.engagement_id

    /* city: pre-derived src key */
    LEFT JOIN 3nf.nf_cities ci
        ON ci.city_src_id = t.city_src_id

    /* store: pre-derived src key */
    LEFT JOIN 3nf.nf_stores st
        ON st.store_src_id = t.store_src_id

    LEFT JOIN 3nf.nf_employees_scd emp
    ON emp.employee_src_id = t.employee_src_id
   AND emp.is_active = TRUE

    WHERE t.transaction_id <> 'n.a.'
      AND t.transaction_dt IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM 3nf.nf_transactions x
          WHERE x.row_sig = t.row_sig
      );

    GET DIAGNOSTICS v_ins_total = ROW_COUNT;

    PERFORM stg.log_etl_event(
        v_proc,
        '3nf.nf_transactions',
        v_ins_total,
        CASE WHEN v_ins_total > 0 THEN 'SUCCESS' ELSE 'NO_CHANGE' END,
        'Inserted=' || v_ins_total || '. Final transaction load from mapping_transactions derived src keys into 3NF surrogate keys.',
        NULL,
        'INFO',
        v_ins_total,
        0,
        0,
        0,
        NULL
    );

    RAISE NOTICE 'ce_transactions completed. inserted=%', v_ins_total;

EXCEPTION
WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS
        v_err_detail = PG_EXCEPTION_DETAIL,
        v_err_hint   = PG_EXCEPTION_HINT;

    v_err_msg := SQLERRM;

    PERFORM stg.log_etl_event(
        v_proc,
        '3nf.nf_transactions',
        0,
        'FAILED',
        'Transaction load failed',
        v_err_msg || ' | DETAIL=' || COALESCE(v_err_detail, 'n.a.')
                  || ' | HINT='   || COALESCE(v_err_hint, 'n.a.'),
        'ERROR',
        0,
        0,
        0,
        0,
        NULL
    );

    RAISE;
END;
$$;

SQL
