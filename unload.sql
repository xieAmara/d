CREATE OR REPLACE PROCEDURE "integration".unload_pu_monthly_wallet_balance_kbzpay_payroll_report()
 LANGUAGE plpgsql
AS $$   
DECLARE var_sql_cmd varchar(4000);
var_file_name varchar(100);
var_today_date date;

    --added variable to rename filename---
var_old_key varchar(1000);
var_new_key varchar(1000);
var_dummy varchar(1000);
BEGIN

var_today_date := (SELECT to_char(public.f_getdate_mmt()::date,'YYYYMMDD'));
 var_file_name:= 'Pu_Monthly_Wallet_Balance_Kbzpay_Payroll_Report_'|| var_today_date;
  var_sql_cmd := N'
        UNLOAD (''

                  SELECT
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."payroll_month" AS "payroll_month",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."bal_month" AS "bal_month",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."employer_short_code" AS "employer_short_code",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."employer_name" AS "employer_name",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."employee_id" AS "employee_id",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."employee_name" AS "employee_name",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."channel" AS "channel",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."has_pay_adv" AS "has_pay_adv",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."account_status" AS "account_status",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."cust_region" AS "cust_region",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."is_bank_acc_link" AS "is_bank_acc_link",
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"."avgerage_balance" AS "avgerage_balance"
                  FROM
                  "acoe_datamart"."pu_monthly_wallet_balance_kbzpay_payroll_report"''
                 )
        to ''s3://acoe-unload/PayrollBU/pu_monthly_wallet_balance_kbzpay_payroll_report/temp/' || var_file_name || '''
        IAM_ROLE ''arn:aws:iam::390295393321:role/service-role/AmazonRedshift-CommandsAccessRole-20230708T204421''
        allowoverwrite
        format CSV
        HEADER
        EXTENSION ''csv''
        parallel off;';

 
  -- raise info 'sql cmd : %', var_sql_cmd;

  EXECUTE var_sql_cmd;
      var_old_key := 'PayrollBU/pu_monthly_wallet_balance_kbzpay_payroll_report/temp/'|| var_file_name|| '000.csv';
        var_new_key := 'PayrollBU/pu_monthly_wallet_balance_kbzpay_payroll_report/'|| var_file_name||'.csv';
        var_dummy := integration.exf_rename_s3_file('acoe-unload', var_old_key, var_new_key);
END;
$$