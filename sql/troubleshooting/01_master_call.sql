-- master call cok uzun sürüyor bazen ancak Call'lar entity based sırasıyla yapılırsa cache probleminden kurtulup bir sorun yaşama ihtimaliniz sıfır. 
-- Buradaki tüm kodlar daha önce denendi ve hepsi çalışıyor.

-- (1) master ingestion load calışmazsa
 
-- raw calls (temizlenmeden csv'den external tabloya)
CALL stg.load_raw_offline();
CALL stg.load_raw_online();

-- src calls (external tablolardan temizlenerek src tablolara)
CALL stg.build_clean_online();
CALL stg.build_clean_offline();

-- (2) mapping calls
CALL stg.load_map_customers();
CALL stg.load_map_stores();
CALL stg.load_map_products();
CALL stg.load_map_engagements();
CALL stg.load_map_deliveries();
CALL stg.load_map_promotions();
CALL stg.load_map_employees();
CALL stg.load_map_transactions();

-- (3) nf calls
-- first lookups
CALL stg.load_ce_states();
CALL stg.load_ce_cities();
CALL stg.load_ce_addresses();
CALL stg.load_ce_product_categories();
CALL stg.load_ce_promotion_types();
CALL stg.load_ce_shipping_partners()     

-- later
CALL stg.load_ce_customers();
CALL stg.load_ce_stores();
CALL stg.load_ce_employees_scd();
CALL stg.load_ce_deliveries();
CALL stg.load_ce_promotions();
CALL stg.load_ce_products();
CALL stg.load_ce_engagements();
CALL stg.load_ce_transactions();

-- (4) dim calls
CALL stg.load_dim_customers();
CALL stg.load_dim_stores();
CALL stg.load_dim_products(); 
CALL stg.load_dim_promotions(); 
CALL stg.load_dim_deliveries();     
CALL stg.load_dim_engagements();
CALL stg.load_dim_dates('2015-01-01'::DATE, '2030-12-31'::DATE); -- based on max and min date in the dataset
CALL stg.load_dim_employees_scd();
CALL stg.master_transactions_monthly_load();

