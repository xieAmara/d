CREATE OR REPLACE PROCEDURE "integration".correct_detail()
 LANGUAGE plpgsql
AS $$

    -- declare variables of today date, and missing dates as counter 
    DECLARE 
    missing_date DATE;
    end_date DATE; 

    var_date DATE; 

    BEGIN
    select dateadd(day, -1, '2025-01-21'::DATE )::date into var_date; 
    select dateadd(day, -1, '2025-01-16'::DATE )::date into missing_date; 
    select dateadd(day, -1, '2025-01-20'::DATE )::date into end_date; 
    
    -- 1. RETRACT 
    -- Loop and delete all incorrect rows and restore to previous status 
    DELETE FROM base.scenerio_test 
    WHERE from_date = var_date; 

    DELETE FROM integration.scenerio_test 
    WHERE iud_op = 'U' OR iud_op = 'I';
    
    WHILE missing_date <= end_date
    LOOP 
        update base.scenerio_test 
            set is_current = 1
                ,"to_date" = dateadd(day, 0, '9999-12-31'::Date)
            where "to_date" = missing_date and is_current = 0;

        missing_date := missing_date + interval '1 day';
        -- SET missing_date = dateadd(day, 1, missing_date);
    END LOOP; 

    -- 2. RESTORE
    -- loop the dates again 
    select dateadd(day, -1, '2025-01-16'::DATE )::date into missing_date; 
    
    WHILE missing_date <= end_date
    LOOP
        -- import data from public to staging 
        truncate table staging.source_db; 

        insert into staging.source_db
        select * from public.source_db
        where data_date = missing_date; 

        -- set hash 
        update staging.source_db 
        set rowhash = MD5(nvl(COALESCE(customer_id::TEXT, ''))
                        ||nvl(customer_name, '')
                        || nvl(COALESCE(customer_id::TEXT, ''))
                        || nvl(email, ''));

        -- import data from staging to integration
        truncate table integration.scenerio_test; 

        insert into integration.scenerio_test(
            customer_id, 
            customer_name, 
            level, 
            email,
            rowhash,
            iud_op
        )
        select 
            s.customer_id, 
            s.customer_name, 
            s.level, 
            s.email,
            s.rowhash,
            'U' as iud_op
            -- case 
            -- when t.customer_id is null then 'I'
            -- when s.customer_id is null then 'D'
            -- when t.rowhash != s.rowhash then 'U'
            -- else 'N' end as iud_op
        from (select *
                    from base.scenerio_test
                    where is_current = 1) t
        inner join staging.source_db s
        on t.customer_id = s.customer_id;

        -- change updated data 
        update base.scenerio_test t
            set is_current = 0
                ,to_date = dateadd(day, -1, missing_date)
            where EXISTS (select s.customer_id from integration.scenerio_test s 
                            where s.iud_op = 'U'
                            and s.customer_id = t.customer_id)
            and is_current = 1; 
        
        -- add data from missing day 
        insert into base.scenerio_test (
        customer_id, 
        customer_name, 
        level, 
        email,
        from_date,
        "to_date",
        is_current, 
        rowhash
        )
        SELECT customer_id,
        customer_name, 
        level,
        email, 
        missing_date as from_date,
        '9999-12-31'::date as "to_date",
        '1' as is_current,
        rowhash
        from integration.scenerio_test
        where iud_op in ('I', 'U');

        missing_date := DATEADD(DAY, 1, missing_date);
    END LOOP;


    END;

    $$