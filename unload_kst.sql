CREATE OR REPLACE PROCEDURE "integration".unload_kbzpay_acquisition_hr_data(param_start_date date, param_end_date date)
 LANGUAGE plpgsql
AS $$
DECLARE
    var_start_date date;
    var_end_date date;
    var_data_date date;
    var_day_key int;
    var_data_date_minus_one date;
    var_day_key_minus_one int;
    var_mtd_date date;
    var_mtd_key int;
    var_codex_date date;
    var_codex_day_key int;
	var_qtd_date date;
    var_file_name varchar(50);
    var_command varchar(3000);
    var_tc_start_date date;
    var_tc_end_date date;
    var_tc_ytd_start_date date; --added on 19 Dec 2024
    var_tc_ytd_end_date date; --added on 19 Dec 2024

    --added variable to rename filename---
    var_old_key varchar(1000);
    var_new_key varchar(1000);
    var_dummy varchar(1000);

BEGIN
    var_codex_date := '2023-10-01'::date;
    var_tc_start_date :='2025-01-03' ::date;
    var_tc_end_date := '2025-04-02' ::date;
    var_tc_ytd_start_date :='2025-01-03' ::date; --added on 19 December by Zin Mar request
    var_tc_ytd_end_date :='2026-01-02' ::date; --added on 19 December by Zin Mar request

 IF param_start_date is null 
    THEN
        select public.f_getdate_mmt()::date into var_start_date;
    ELSE
        var_start_date := param_start_date;
    END IF;

    IF param_end_date is null 
    THEN
        select public.f_getdate_mmt()::date into var_end_date;
    ELSE 
        var_end_date := param_end_date;
    END IF;

    WHILE var_start_date <= var_end_date LOOP 
    
        var_data_date := var_start_date;
        var_data_date_minus_one := dateadd(day, -1, var_data_date)::date;
       

        var_file_name := concat('kbzpay_acquisition_hr_data_', var_data_date_minus_one);
		
		/*Add new variable to get quarter of data date */
		var_qtd_date := DATE_TRUNC('QUARTER', var_data_date_minus_one) ::date;

        drop table if exists #tmp_pay_acquisition;
        /**
        Updated tablename and add condition 
        on 2 Feb 2024
        **/
         with 
        emp_data as 
        (
        select Employee_ID,Employee_Full_Name,agentid,phonenumber,dateofbirth,gender,dateofjoining,serviceyears,Branch_Code,
        Branch_name,joblocationstate,joblocationtownship,Center_Type from 
        (
        select Employee_ID,Employee_Full_Name,Branch_Code,Branch_name,CONCAT('0',RED_PHONE) as agentid,
        CASE 
        WHEN LEFT(Branch_Code, 1) = 'F' THEN 'FUNCTION'
        WHEN LEFT(Branch_Code, 1) = 'V' THEN 'BUSINESS UNIT'
        WHEN LEFT(Branch_Code, 1) = 'C' THEN 'PAY CENTER'
        ELSE 'BRANCH'END AS Center_Type --adde don 19 December by Zin Mar request
        FROM tc_external_schema.tc_employee_eligible_list
        where file_date in (SELECT MAX(file_date) from tc_external_schema.tc_employee_eligible_list)
        )a 
        left join 
        (
        select employeeid,f_hr_get_service_years(dateofjoining, effectivedate) as serviceyears,dateofjoining,gender,dateofbirth,nationalityid,phonenumber,joblocationstate,joblocationtownship
        from kbz_analytics.base.hremployeeinfo
        where iscurrent = 1
        )b on a.Employee_ID = b.employeeid
        ),

        cust_count as (
        select org_short_code, 
        COUNT (distinct case when approval_date = var_data_date_minus_one then cust_id end) as Cal_D_1,
        COUNT (distinct case when approval_date BETWEEN var_qtd_date and var_data_date_minus_one then cust_id end) as Cal_QTD,
        COUNT (distinct case 
            when 
            approval_date between DATEADD(day, -(DATEPART(DOW, var_data_date_minus_one)), var_data_date_minus_one)::date and var_data_date_minus_one
            then cust_id
            end) as Cal_WTD,
        COUNT (distinct case 
        when 
        DATE_PART('month', approval_date) = DATE_PART('month', var_data_date_minus_one) 
        and EXTRACT(YEAR FROM approval_date) = date_part_year(trunc(getdate())) 
        then cust_id
        end) as Cal_MTD,
		COUNT (distinct case when approval_date BETWEEN var_codex_date and var_data_date_minus_one then cust_id end) as BTD,
        COUNT (distinct case when approval_date BETWEEN var_tc_ytd_start_date and var_tc_ytd_end_date then cust_id end) as TC_YTD
        from kbz_analytics.base.kpay_cust_reg_details
        where acc_status <>'Closed'
        and approval_date BETWEEN var_codex_date and var_data_date_minus_one
        group by org_short_code
        ),
		---added on 8 Mar 2024-------------
		TC_cust_count as (
		select org_short_code,
		COUNT (distinct case when approval_date = var_data_date_minus_one then cust_id end) as TC_D_1,
		COUNT (distinct case when approval_date BETWEEN var_qtd_date and var_data_date_minus_one then cust_id end) as TC_QTD,
		COUNT (distinct case 
			when 
			approval_date between DATEADD(day, -1*(DATEPART(WEEKDAY, var_data_date_minus_one)), var_data_date_minus_one)::date and var_data_date_minus_one
			then cust_id
			end) as TC_WTD,
		COUNT (distinct case 
			when 
			DATE_PART('month', approval_date) = DATE_PART('month', var_data_date_minus_one) 
			and EXTRACT(YEAR FROM approval_date) = date_part_year(trunc(getdate())) 
			then cust_id
			end) as TC_MTD
		from kbz_analytics.base.kpay_cust_reg_details
		where acc_status <>'Closed'
		and approval_date BETWEEN var_tc_start_date and var_tc_end_date
		group by org_short_code
	 )

        select Employee_ID,Employee_Full_Name,agentid,phonenumber,dateofbirth,gender,dateofjoining,serviceyears,Branch_Code,
        Branch_name,joblocationstate,joblocationtownship,TC_D_1,TC_WTD,TC_MTD,TC_QTD,BTD,Cal_D_1,Cal_WTD,Cal_MTD,Cal_QTD,TC_YTD,Center_Type
        into #tmp_pay_acquisition
        from emp_data
        left join cust_count on emp_data.agentid = cust_count.org_short_code
		left join TC_cust_count on emp_data.agentid = TC_cust_count.org_short_code;

        var_command := N'unload (''select * from #tmp_pay_acquisition'') 
        to ''s3://acoe-datalake-processed/codex/temp/' || var_file_name || '''
        iam_role ''arn:aws:iam::390295393321:role/service-role/AmazonRedshift-CommandsAccessRole-20230708T204421''
        allowoverwrite
        format CSV
        HEADER
        EXTENSION ''csv''
        parallel off;';

        execute var_command;
        --added on 09 Sept 2024 
        --to rename "kbzpay_acquisition_hr_data_2024-09-08000.csv"  to "kbzpay_acquisition_hr_data_2024-09-08.csv"
        var_old_key := 'codex/temp/'|| var_file_name|| '000.csv';
        var_new_key := 'codex/KBZACoEDataSets/'|| var_file_name||'.csv';
        var_dummy := integration.exf_rename_s3_file('acoe-datalake-processed', var_old_key, var_new_key);
        var_start_date := dateadd(day, 1, var_start_date);   
        
    END LOOP;
END;
$$