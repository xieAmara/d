CREATE OR REPLACE PROCEDURE "integration".to_base_kpay_market()  
LANGUAGE plpgsql AS $$      
DECLARE
    var_start_date DATE;
    var_end_date DATE;
    formatted_date TEXT;
    file TEXT;
    exist BOOLEAN; 
BEGIN
    -- Set start and end date
    var_start_date := (SELECT dateadd(day, -1, '2023-06-17'::DATE )::date);
    var_end_date := (SELECT dateadd(day, -1, '2025-02-21'::DATE )::date);

    -- Loop through dates from start to end
    WHILE var_start_date <= var_end_date LOOP
        -- Format the current date as YYYYMMDD
        formatted_date := to_char(var_start_date, 'YYYYMMDD');
        file := 'Category_of_KBZPay_Market_' || formatted_date;

        -- Check if data exists for this day
        SELECT EXISTS (SELECT 1 FROM dl_processed_test.kbzpay_market WHERE day_key = formatted_date) 
        INTO exist;

        -- If data exists, process it
        IF exist THEN
            -- Truncate staging table to prepare for the new data
            TRUNCATE TABLE staging.kbzpay_market;

            -- Move data from processed to staging depending on the day_key
            INSERT INTO staging.kbzpay_market
            SELECT * 
            FROM dl_processed_test.kbzpay_market 
            WHERE day_key = formatted_date;

            -- Update existing records in base.kbzpay_market if there are changes
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
            FROM staging.kbzpay_market s
            WHERE (t.level_3_category_id = s.level_3_category_id AND t.level_1_category_id = s.level_1_category_id AND t.level_2_category_id = s.level_2_category_id)
            AND (t.status <> s.status); 

            -- Insert new records that do not already exist in base.kbzpay_market
            INSERT INTO base.kbzpay_market (
                level_1_category_id, level_1_category_name, level_2_category_id, 
                level_2_category_name, level_3_category_id, level_3_category_name, 
                created_user_id, created_date, status, updated_user_id, 
                last_updated_date, file_name, day_key
            )
            SELECT 
                level_1_category_id, level_1_category_name, level_2_category_id, 
                level_2_category_name, level_3_category_id, level_3_category_name, 
                created_user_id, created_date, status, updated_user_id, 
                last_updated_date, file_name, day_key
            FROM staging.kbzpay_market s
            WHERE NOT EXISTS (
                SELECT 1 
                FROM base.kbzpay_market t
                WHERE t.level_1_category_id = s.level_1_category_id
                  AND t.level_1_category_name = s.level_1_category_name
                  AND t.level_2_category_id = s.level_2_category_id
                  AND t.level_2_category_name = s.level_2_category_name
                  AND t.level_3_category_id = s.level_3_category_id
                  AND t.level_3_category_name = s.level_3_category_name
                  AND t.created_user_id = s.created_user_id
                  AND t.status = s.status
                  AND t.updated_user_id = s.updated_user_id
            );

        ELSE
            -- Log skipped file if already processed
            RAISE NOTICE 'Skipping file: % for date % (already processed)', file, var_start_date;
        END IF;

        -- Increment the date for the next iteration
        var_start_date := DATEADD(DAY, 1, var_start_date);
    END LOOP;

END;   
$$;
