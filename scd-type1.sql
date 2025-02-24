CREATE OR REPLACE PROCEDURE "integration".to_base_kpay_market()
 LANGUAGE plpgsql
AS $$
    
    BEGIN 

    truncate table staging.kbzpay_market;
    -- start by moving data from processed to staging depending on the daykey 
    INSERT INTO staging.kbzpay_market
    SELECT *
    FROM dl_processed_test.kbzpay_market
    WHERE day_key = '20230616';

    -- update if there is a change to the value 
    UPDATE base.kbzpay_market t
    SET 
        level_1_category_id = s.level_1_category_id,
        level_1_category_name = s.level_1_category_name,
        level_2_category_id = s.level_2_category_id,
        level_2_category_name = s.level_2_category_name,
        level_3_category_id = s.level_3_category_id,
        level_3_category_name = s.level_3_category_name,
        created_user_id = s.created_user_id,
        created_date = s.created_date,
        status = s.status,
        updated_user_id = s.updated_user_id,
        last_updated_date = s.last_updated_date,
        file_name = s.file_name,
        day_key = s.day_key
    FROM 
        staging.kbzpay_market s
    WHERE (t.level_3_category_id = s.level_3_category_id AND t.level_1_category_id = s.level_1_category_id AND t.level_2_category_id = s.level_2_category_id)
     AND (t.status <> s.status);
    
    -- insert new values 
    INSERT INTO base.kbzpay_market (
    level_1_category_id, level_1_category_name, level_2_category_id, 
    level_2_category_name, level_3_category_id, level_3_category_name, 
    created_user_id, created_date, status, updated_user_id, 
    last_updated_date, file_name, day_key 
    )
    SELECT level_1_category_id, level_1_category_name, level_2_category_id, 
    level_2_category_name, level_3_category_id, level_3_category_name, 
    created_user_id, created_date, status, updated_user_id, 
    last_updated_date, file_name, day_key 
    FROM staging.kbzpay_market s
    WHERE NOT EXISTS (
        SELECT 1 
        FROM base.kbzpay_market t
        WHERE 
            t.level_1_category_id = s.level_1_category_id AND  
            t.level_1_category_name = s.level_1_category_name AND
            t.level_2_category_id = s.level_2_category_id AND
            t.level_2_category_name = s.level_2_category_name AND
            t.level_3_category_id = s.level_3_category_id AND
            t.level_3_category_name = s.level_3_category_name AND
            t.created_user_id = s.created_user_id AND
            t.status = s.status AND
            t.updated_user_id = s.updated_user_id 
    ); 
    END;

$$