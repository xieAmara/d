CREATE OR REPLACE PROCEDURE public.extract_phone_numbers()
LANGUAGE plpgsql
AS $$ 
DECLARE 
    max_num_phone INT;
    row_record RECORD; -- Use RECORD to hold each row of results
    i INT := 0;
    sql_query VARCHAR(5000) := 'SELECT case_no, ';  
BEGIN

    -- Loop over each row to get the number of phone numbers
    FOR row_record IN
        SELECT 
            json_array_length(json_extract_path_text(second_related_phone, 'second_related_phone_json')) AS num_phones, 
            case_no
        FROM dl_processed_test.kpay_fraud
        where case_no = 'P6375/2023'
    LOOP
       
        IF row_record.num_phones IS NOT NULL THEN
           
            FOR i IN 0..row_record.num_phones-1 LOOP
                sql_query := sql_query || 
                    'json_extract_path_text(second_related_phone, ''second_related_phone_json'', ''' || i || ''') AS phone_number_'|| i ||',';  
            END LOOP;
           
            sql_query := LEFT(sql_query, LENGTH(sql_query) - 1); 
           
            sql_query := sql_query || ' FROM dl_processed_test.kpay_fraud WHERE case_no = ''' || row_record.case_no || ''';'; 

            RAISE NOTICE 'Generated SQL Query: %', sql_query;

            sql_query := 'SELECT case_no, ';  
        END IF;
    END LOOP;

END $$;

CALL public.extract_phone_numbers();