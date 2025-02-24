DECLARE
    var_data_date DATE;
    var_base_max_date DATE;
    var_start_date DATE;

BEGIN
    -- Get D-1 (yesterday's date)
    var_data_date := (SELECT DATEADD(DAY, -1, public.f_getdate_mmt())::DATE);
    
    var_base_max_date := (SELECT cast(max(day_key) as varchar(10))::date FROM base.market_category);

    -- Get max date from base table
    
    var_start_date := DATEADD(DAY, 1, var_base_max_date);

    -- Loop through dates
    WHILE var_start_date <= var_data_date LOOP

        -- Delete existing records
        DELETE FROM base.market_category
        USING dl_processed.market_category
        WHERE 
            dl_processed.market_category.day_key::DATE = var_start_date
            AND base.market_category.level1_category_id = dl_processed.market_category.level1_category_id
            AND base.market_category.level2_category_id = dl_processed.market_category.level2_category_id
            AND base.market_category.level3_category_id = dl_processed.market_category.level3_category_id
            AND base.market_category.status = dl_processed.market_category.status;

        -- Insert new records
        INSERT INTO base.market_category(
            level1_category_id,
            level1_category_name,
            level2_category_id,
            level2_category_name,
            level3_category_id,
            level3_category_name,
            created_user_id,
            created_date,
            status,
            updated_user_id,
            last_updated_date,
            filename,
            year_key,
            month_key,
            day_key
        )
        SELECT 
            level1_category_id,
            level1_category_name,
            level2_category_id,
            level2_category_name,
            level3_category_id,
            level3_category_name,
            created_user_id,
            max(created_date) as created_date,
            status,
            updated_user_id,
            max(last_updated_date) as last_updated_date,
            filename,
            CAST(year_key AS INT) as year_key,
            CAST(month_key AS INT) as month_key,
            CAST(day_key AS INT) as day_key
        FROM dl_processed.market_category
        WHERE day_key::DATE = var_start_date
        group by 
         level1_category_id,
            level1_category_name,
            level2_category_id,
            level2_category_name,
            level3_category_id,
            level3_category_name,
            created_user_id,
            status,
            updated_user_id,
            filename,
            day_key,
            month_key,
            year_key;

        -- Call audit rowcount procedure
        CALL integration.usp_audit_rowcount(pg_last_query_id(), 'integration.to_base_market_category()');

        -- Update blank columns to NULL
        CALL integration.column_blank_to_null('base', 'market_category', NULL);

        -- Increment date
        var_start_date := DATEADD(DAY, 1, var_start_date);
    END LOOP;

END;