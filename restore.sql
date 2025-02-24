CREATE OR REPLACE PROCEDURE "integration".correct_detail()
 LANGUAGE plpgsql
AS $$

    -- add a variable for today's date (20 Jan 2025)
    DECLARE 
    var_date date;
    missing_date date; 

    BEGIN

    /* Would retrieve CURRDATE() for real scenerio */
    select dateadd(day, -1, '2025-01-20'::DATE )::date into var_date; 
    select dateadd(day, -1, var_date)::date into missing_date; 
    
    -- remove rows of 20 Jan in base and integration 

    DELETE FROM base.scenerio_test 
    WHERE from_date = var_date;

    DELETE FROM integration.scenerio_test 
    WHERE iud_op = 'U' or iud_op = 'I';

    -- insert data from 19 Jan into staging 
    
    truncate table staging.source_db; 
    
    INSERT INTO staging.source_db
    SELECT *
    FROM public.source_db
    WHERE data_date = missing_date;

    -- set rowhash
    update staging.source_db
    set rowhash = MD5(nvl(COALESCE(customer_id::TEXT, ''))
                        ||nvl(customer_name, '')
                        || nvl(COALESCE(customer_id::TEXT, ''))
                        || nvl(email, ''));

    -- import data from staging to integration with missing date (19 Jan)
    
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
    full outer join staging.source_db s
    on t.customer_id = s.customer_id;

    -- set data in base to status before 20th Jan 
    update base.scenerio_test 
            set is_current = 1
                ,"to_date" = dateadd(day, 0, '9999-12-31'::Date)
            where "to_date" = missing_date and is_current = 0;
        
    -- change the updated data 
    update base.scenerio_test t
            set is_current = 0
                ,to_date = dateadd(day, -1, missing_date)
            where EXISTS (select s.customer_id from integration.scenerio_test s 
                            where s.iud_op = 'U'
                            and s.customer_id = t.customer_id)
            and is_current = 1; 

    -- add data from 19th Jan 

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

    END;

    $$