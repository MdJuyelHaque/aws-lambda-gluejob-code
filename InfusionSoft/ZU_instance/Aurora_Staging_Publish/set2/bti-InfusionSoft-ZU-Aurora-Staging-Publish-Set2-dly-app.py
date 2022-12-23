print("job started")
import sys
from awsglue.utils import getResolvedOptions
import mysql.connector
import boto3
args= getResolvedOptions(sys.argv, ['job_name'])
name=args.get('job_name')


client = boto3.client('ssm')
password = client.get_parameter(
            Name='bti_staging_db_password',
            WithDecryption=True
            )['Parameter']['Value']
user = client.get_parameter(
            Name='bti_staging_db-username',
            WithDecryption=True
            )['Parameter']['Value']
host = client.get_parameter(
            Name='bti_staging_db_url',
            WithDecryption=True
            )['Parameter']['Value']

                    
def contacts():
    try:
        args= getResolvedOptions(sys.argv, ['Cont_source_table','Cont_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Cont_source_table')
        target_table=args.get('Cont_target_table')
        mySql_insert_query = """ replace INTO {}
                (upd_date,
                upd_user, 
                last_updated_utc_millis,
                id,
                custom_fields_id_0,
                custom_fields_id_1, 
                custom_fields_id_2,
                custom_fields_id_3,
                custom_fields_id_4, 
                custom_fields_id_5,
                custom_fields_id_6, 
                custom_fields_id_7,
                custom_fields_id_8,
                custom_fields_id_9,
                custom_fields_id_10,
                custom_fields_id_11,
                custom_fields_id_12,
                custom_fields_id_13,
                custom_fields_id_14,
                custom_fields_id_15,
                custom_fields_id_16,
                custom_fields_id_17,
                custom_fields_id_18,
                custom_fields_id_19,
                custom_fields_id_20,
                custom_fields_id_21,
                custom_fields_id_22,
                custom_fields_id_23,
                custom_fields_id_24,
                custom_fields_id_25,
                custom_fields_id_26,
                custom_fields_id_27,
                custom_fields_id_28,
                custom_fields_id_29,
                custom_fields_id_30,
                custom_fields_id_31, 
                custom_fields_id_32,
                custom_fields_id_33,
                custom_fields_id_34,
                custom_fields_id_35,
                custom_fields_id_36,
                custom_fields_id_37, 
                custom_fields_id_38, 
                custom_fields_id_39,
                custom_fields_id_40,
                custom_fields_id_41,
                custom_fields_id_42,
                custom_fields_id_43, 
                custom_fields_id_44, 
                custom_fields_id_45, 
                custom_fields_id_46, 
                custom_fields_id_47, 
                custom_fields_id_48, 
                custom_fields_id_49, 
                custom_fields_id_50, 
                custom_fields_id_51, 
                custom_fields_id_52,
                last_updated,
                tag_ids, 
                date_created, 
                given_name,
                email_addresses_email_0,
                email_addresses_field_0, 
                custom_fields_content_0,
                custom_fields_content_1,
                custom_fields_content_2, 
                custom_fields_content_3,
                custom_fields_content_4,
                custom_fields_content_7,
                custom_fields_content_8, 
                custom_fields_content_9, 
                email_opted_in)
                select
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`, 
                last_updated_utc_millis,
                id,
                custom_fields_id_0,
                custom_fields_id_1, 
                custom_fields_id_2,
                custom_fields_id_3,
                custom_fields_id_4, 
                custom_fields_id_5,
                custom_fields_id_6, 
                custom_fields_id_7,
                custom_fields_id_8,
                custom_fields_id_9,
                custom_fields_id_10,
                custom_fields_id_11,
                custom_fields_id_12,
                custom_fields_id_13,
                custom_fields_id_14,
                custom_fields_id_15,
                custom_fields_id_16,
                custom_fields_id_17,
                custom_fields_id_18,
                custom_fields_id_19,
                custom_fields_id_20,
                custom_fields_id_21,
                custom_fields_id_22,
                custom_fields_id_23,
                custom_fields_id_24,
                custom_fields_id_25,
                custom_fields_id_26,
                custom_fields_id_27,
                custom_fields_id_28,
                custom_fields_id_29,
                custom_fields_id_30,
                custom_fields_id_31, 
                custom_fields_id_32,
                custom_fields_id_33,
                custom_fields_id_34,
                custom_fields_id_35,
                custom_fields_id_36,
                custom_fields_id_37, 
                custom_fields_id_38, 
                custom_fields_id_39,
                custom_fields_id_40,
                custom_fields_id_41,
                custom_fields_id_42,
                custom_fields_id_43, 
                custom_fields_id_44, 
                custom_fields_id_45, 
                custom_fields_id_46, 
                custom_fields_id_47, 
                custom_fields_id_48, 
                custom_fields_id_49, 
                custom_fields_id_50, 
                custom_fields_id_51, 
                custom_fields_id_52,
                cast(last_updated as datetime) as last_updated,
                tag_ids, 
                cast(date_created as datetime) as date_created, 
                given_name,
                email_addresses_email_0,
                email_addresses_field_0, 
                custom_fields_content_0,
                custom_fields_content_1,
                custom_fields_content_2, 
                custom_fields_content_3,
                custom_fields_content_4,
                custom_fields_content_7,
                custom_fields_content_8, 
                custom_fields_content_9, 
                email_opted_in
                from {}""".format(target_table,source_table)
        
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 

def countries():
    try:
        args= getResolvedOptions(sys.argv, ['Cntry_source_table','Cntry_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Cntry_source_table')
        target_table=args.get('Cntry_target_table')
        mySql_insert_query = """ replace INTO {}
                (`country_code`, 
                `country_name`,
                `upd_user`,
                `upd_date`)
                select
                `country_code`, 
                `country_name`,  
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 


def files():
    try:
        args= getResolvedOptions(sys.argv, ['File_source_table','File_target_table','database','port'])
    
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('File_source_table')
        target_table=args.get('File_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
                `file_size`, 
                `created_by`, 
                `contact_id`, 
                `category`, 
                `date_created`, 
                `last_updated`, 
                `file_name`, 
                `remote_file_key`, 
                `file_box_type`, 
                `download_url`, 
                `public`,
                `upd_user`,
                `upd_date`)
                select
                `id`, 
                `file_size`, 
                `created_by`, 
                `contact_id`, 
                `category`, 
                cast(date_created as date) as date_created, 
                cast(last_updated as date) as last_updated, 
                `file_name`, 
                `remote_file_key`, 
                `file_box_type`, 
                `download_url`, 
                `public`,  
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 


def merchants():
    try:
        args= getResolvedOptions(sys.argv, ['Mer_source_table','Mer_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Mer_source_table')
        target_table=args.get('Mer_target_table')
        mySql_insert_query = """ replace INTO {}
                (`default_merchant_account`, 
                `merchant_accounts_id`, 
                `merchant_accounts_type`, 
                `merchant_accounts_account_name`, 
                `merchant_accounts_is_test`,
                `upd_user`,
                `upd_date`)
                select
                `default_merchant_account`, 
                `merchant_accounts_id`, 
                `merchant_accounts_type`, 
                `merchant_accounts_account_name`, 
                `merchant_accounts_is_test`,  
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 


def opportunities():
    try:
        args= getResolvedOptions(sys.argv, ['Opp_source_table','Opp_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Opp_source_table')
        target_table=args.get('Opp_target_table')
        mySql_insert_query = """ replace INTO {}
                (`affiliate_id`, 
                `stage_id`,
                `contact_id`, 
                `include_in_forecast`, 
                `id`,  
                `user_id`, 
                `last_updated`, 
                `opportunity_notes`,
                `opportunity_title`, 
                `date_created`, 
                `estimated_close_date`, 
                `next_action_notes`, 
                `stage_name`, 
                `stage_details`, 
                `next_action_date`, 
                `contact_email`, 
                `contact_first_name`, 
                `contact_last_name`, 
                `contact_company_name`, 
                `contact_job_title`, 
                `contact_phone_number`, 
                `projected_revenue_low`, 
                `user_first_name`, 
                `user_last_name`, 
                `projected_revenue_high`, 
                `upd_user`, 
                `upd_date`)
                select
                `affiliate_id`, 
                `stage_id`,
                `contact_id`, 
                `include_in_forecast`, 
                `id`,  
                `user_id`, 
                cast(last_updated as datetime) as last_updated, 
                `opportunity_notes`, 
                `opportunity_title`, 
                cast(date_created as datetime) as date_created,
                cast(estimated_close_date as datetime) as estimated_close_date, 
                `next_action_notes`, 
                `stage_name`, 
                `stage_details`,
                cast(next_action_date as datetime) as next_action_date,
                `contact_email`, 
                `contact_first_name`, 
                `contact_last_name`, 
                `contact_company_name`, 
                `contact_job_title`, 
                `contact_phone_number`, 
                `projected_revenue_low`, 
                `user_first_name`, 
                `user_last_name`, 
                `projected_revenue_high`,
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 

def orders():
    try:
        args= getResolvedOptions(sys.argv, ['Ord_source_table','Ord_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Ord_source_table')
        target_table=args.get('Ord_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
                `contact_id`, 
                `lead_affiliate_id`, 
                `sales_affiliate_id`, 
                `shipping_information_id`, 
                `order_items_id`, 
                `order_items_jobRecurringId`, 
                `order_items_quantity`, 
                `order_items_specialId`, 
                `order_items_specialPctOrAmt`, 
                `order_items_product_id`, 
                `payment_plan_credit_card_id`, 
                `payment_plan_days_between_payments`, 
                `payment_plan_number_of_payments`, 
                `payment_plan_payment_gateway_merchant_account_id`, 
                `title`, 
                `status`, 
                `recurring`, 
                `total`, 
                `contact_email`, 
                `contact_first_name`, 
                `contact_last_name`, 
                `contact_company_name`, 
                `contact_job_title`, 
                `notes`, 
                `terms`, 
                `source_type`, 
                `creation_date`, 
                `modification_date`, 
                `order_date`, 
                `total_paid`, 
                `total_due`, 
                `shipping_information_first_name`, 
                `shipping_information_middle_name`, 
                `shipping_information_last_name`, 
                `shipping_information_company`, 
                `shipping_information_phone`, 
                `shipping_information_street1`, 
                `shipping_information_street2`, 
                `shipping_information_city`, 
                `shipping_information_state`, 
                `shipping_information_zip`, 
                `shipping_information_country`, 
                `refund_total`, 
                `allow_payment`, 
                `allow_paypal`, 
                `order_items_name`, 
                `order_items_description`, 
                `order_items_type`, 
                `order_items_notes`, 
                `order_items_cost`, 
                `order_items_price`, 
                `order_items_discount`, 
                `order_items_specialAmount`, 
                `order_items_product_name`, 
                `order_items_product_sku`, 
                `order_items_product_description`, 
                `order_items_subscriptionPlan`, 
                `payment_plan_initial_payment_amount`, 
                `payment_plan_initial_payment_date`, 
                `payment_plan_plan_start_date`, 
                `shipping_information_invoiceToCompany`, 
                `order_items_product_shippable`, 
                `order_items_product_taxable`, 
                `payment_plan_auto_charge`, 
                `payment_plan_payment_gateway_use_default`,
                `upd_user`, 
                `upd_date`)
                select
                `id`, 
                `contact_id`, 
                `lead_affiliate_id`, 
                `sales_affiliate_id`, 
                `shipping_information_id`, 
                `order_items_id`, 
                `order_items_jobRecurringId`, 
                `order_items_quantity`, 
                `order_items_specialId`, 
                `order_items_specialPctOrAmt`, 
                `order_items_product_id`, 
                `payment_plan_credit_card_id`, 
                `payment_plan_days_between_payments`, 
                `payment_plan_number_of_payments`, 
                `payment_plan_payment_gateway_merchant_account_id`, 
                `title`, 
                `status`, 
                `recurring`, 
                `total`, 
                `contact_email`, 
                `contact_first_name`, 
                `contact_last_name`, 
                `contact_company_name`, 
                `contact_job_title`, 
                `notes`, 
                `terms`, 
                `source_type`, 
                cast(creation_date as datetime) as creation_date, 
                cast(modification_date as datetime) as modification_date, 
                cast(order_date as datetime) as order_date, 
                `total_paid`, 
                `total_due`, 
                `shipping_information_first_name`, 
                `shipping_information_middle_name`, 
                `shipping_information_last_name`, 
                `shipping_information_company`, 
                `shipping_information_phone`, 
                `shipping_information_street1`, 
                `shipping_information_street2`, 
                `shipping_information_city`, 
                `shipping_information_state`, 
                `shipping_information_zip`, 
                `shipping_information_country`, 
                `refund_total`, 
                `allow_payment`, 
                `allow_paypal`, 
                `order_items_name`, 
                `order_items_description`, 
                `order_items_type`, 
                `order_items_notes`, 
                `order_items_cost`, 
                `order_items_price`, 
                `order_items_discount`, 
                `order_items_specialAmount`, 
                `order_items_product_name`, 
                `order_items_product_sku`, 
                `order_items_product_description`, 
                `order_items_subscriptionPlan`, 
                `payment_plan_initial_payment_amount`, 
                cast(payment_plan_initial_payment_date as datetime) as payment_plan_initial_payment_date,
                cast(payment_plan_plan_start_date as date) as payment_plan_plan_start_date, 
                `shipping_information_invoiceToCompany`, 
                `order_items_product_shippable`, 
                `order_items_product_taxable`, 
                `payment_plan_auto_charge`, 
                `payment_plan_payment_gateway_use_default`,
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 


def products():
    try:
        args= getResolvedOptions(sys.argv, ['Prod_source_table','Prod_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Prod_source_table')
        target_table=args.get('Prod_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
                `status`, 
                `sub_category_id`, 
                `sku`, 
                `url`, 
                `product_name`, 
                `product_desc`, 
                `product_price`, 
                `product_short_desc`, 
                `subscription_plans_id`, 
                `subscription_plans_cycle`, 
                `subscription_plans_frequency`, 
                `subscription_plans_url`, 
                `subscription_plans_active`, 
                `subscription_plans_subscription_plan_name`, 
                `subscription_plans_number_of_cycles`, 
                `subscription_plans_plan_price`, 
                `subscription_plans_subscription_plan_index`, 
                `subscription_only`,
                `upd_user`,
                `upd_date`)
                select
                `id`, 
                `status`, 
                `sub_category_id`, 
                `sku`, 
                `url`, 
                `product_name`, 
                `product_desc`, 
                `product_price`, 
                `product_short_desc`, 
                `subscription_plans_id`, 
                `subscription_plans_cycle`, 
                `subscription_plans_frequency`, 
                `subscription_plans_url`, 
                `subscription_plans_active`, 
                `subscription_plans_subscription_plan_name`, 
                `subscription_plans_number_of_cycles`, 
                `subscription_plans_plan_price`, 
                `subscription_plans_subscription_plan_index`, 
                `subscription_only`,
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 


def subscriptions():
    try:
        args= getResolvedOptions(sys.argv, ['Sub_source_table','Sub_target_table','database','port'])
    
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Sub_source_table')
        target_table=args.get('Sub_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
                `quantity`, 
                `contact_id`, 
                `product_id`, 
                `subscription_plan_id`, 
                `billing_frequency`, 
                `payment_gateway_id`, 
                `credit_card_id`, 
                `sale_affiliate_id`, 
                `billing_amount`, 
                `billing_cycle`, 
                `start_date`, 
                `next_bill_date`, 
                `end_date`, 
                `active`, 
                `auto_charge`, 
                `use_default_payment_gateway`, 
                `allow_tax`,
                `upd_user`,
                `upd_date`)
                select
                `id`, 
                `quantity`, 
                `contact_id`, 
                `product_id`, 
                `subscription_plan_id`, 
                `billing_frequency`, 
                `payment_gateway_id`, 
                `credit_card_id`, 
                `sale_affiliate_id`, 
                `billing_amount`, 
                `billing_cycle`,
                cast(start_date as date) as start_date, 
                cast(next_bill_date as date) as next_bill_date,
                cast(end_date as date) as end_date,
                `active`, 
                `auto_charge`, 
                `use_default_payment_gateway`, 
                `allow_tax`,
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 


def tags():
    try:
        args= getResolvedOptions(sys.argv, ['Tags_source_table','Tags_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Tags_source_table')
        target_table=args.get('Tags_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
                `name`, 
                `description`,
                `category`,
                `category_id`, 
                `category_name`, 
                `category_description`, 
                `upd_user`,
                `upd_date`)
                select
                `id`, 
                `name`, 
                `description`,
                `category`,  
                `category_id`, 
                `category_name`, 
                `category_description`,
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 

def transactions():
    try:
        args= getResolvedOptions(sys.argv, ['Trans_source_table','Trans_target_table','database','port'])

        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Trans_source_table')
        target_table=args.get('Trans_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
                `orders_id`, 
                `orders_contact_id`, 
                `orders_lead_affiliate_id`, 
                `orders_sales_affiliate_id`, 
                `orders_shipping_information_id`, 
                `orders_shipping_information_zip`, 
                `orders_order_items_id`, 
                `orders_order_items_jobRecurringId`, 
                `orders_order_items_quantity`, 
                `orders_order_items_specialId`, 
                `orders_order_items_specialPctOrAmt`, 
                `orders_order_items_product_id`, 
                `contact_id`, 
                `order_ids`, 
                `payment_id`, 
                `amount`, 
                `currency`, 
                `gateway`, 
                `type`, 
                `status`, 
                `errors`, 
                `orders_title`, 
                `orders_status`, 
                `orders_recurring`, 
                `orders_total`, 
                `orders_contact_email`, 
                `orders_contact_first_name`, 
                `orders_contact_last_name`, 
                `orders_contact_company_name`, 
                `orders_contact_job_title`, 
                `orders_notes`, 
                `orders_terms`, 
                `orders_source_type`, 
                `orders_creation_date`, 
                `orders_modification_date`, 
                `orders_order_date`, 
                `orders_total_paid`, 
                `orders_total_due`, 
                `orders_shipping_information_first_name`, 
                `orders_shipping_information_middle_name`, 
                `orders_shipping_information_last_name`, 
                `orders_shipping_information_company`, 
                `orders_shipping_information_phone`, 
                `orders_shipping_information_street1`, 
                `orders_shipping_information_street2`, 
                `orders_shipping_information_city`, 
                `orders_shipping_information_state`, 
                `orders_shipping_information_country`, 
                `orders_refund_total`, 
                `orders_allow_payment`, 
                `orders_allow_paypal`, 
                `orders_order_items_name`, 
                `orders_order_items_description`, 
                `orders_order_items_type`, 
                `orders_order_items_notes`, 
                `orders_order_items_cost`, 
                `orders_order_items_price`, 
                `orders_order_items_discount`, 
                `orders_order_items_specialAmount`, 
                `orders_order_items_product_name`, 
                `orders_order_items_product_sku`, 
                `orders_order_items_product_description`, 
                `orders_order_items_subscriptionPlan`, 
                `orders_payment_plan`, 
                `transaction_date`, 
                `gateway_account_name`, 
                `collection_method`, 
                `test`, 
                `orders_shipping_information_invoiceToCompany`, 
                `orders_order_items_product_shippable`, 
                `orders_order_items_product_taxable`, 
                `upd_user`,
                `upd_date`)
                select
                `id`, 
                `orders_id`, 
                `orders_contact_id`, 
                `orders_lead_affiliate_id`, 
                `orders_sales_affiliate_id`, 
                `orders_shipping_information_id`, 
                `orders_shipping_information_zip`, 
                `orders_order_items_id`, 
                `orders_order_items_jobRecurringId`, 
                `orders_order_items_quantity`, 
                `orders_order_items_specialId`, 
                `orders_order_items_specialPctOrAmt`, 
                `orders_order_items_product_id`, 
                `contact_id`, 
                `order_ids`, 
                `payment_id`, 
                `amount`, 
                `currency`, 
                `gateway`, 
                `type`, 
                `status`, 
                `errors`, 
                `orders_title`, 
                `orders_status`, 
                `orders_recurring`, 
                `orders_total`, 
                `orders_contact_email`, 
                `orders_contact_first_name`, 
                `orders_contact_last_name`, 
                `orders_contact_company_name`, 
                `orders_contact_job_title`, 
                `orders_notes`, 
                `orders_terms`, 
                `orders_source_type`, 
                cast(orders_creation_date as datetime) as orders_creation_date, 
                cast(orders_modification_date as datetime) as orders_modification_date, 
                cast(orders_order_date as datetime) as orders_order_date,  
                `orders_total_paid`, 
                `orders_total_due`, 
                `orders_shipping_information_first_name`, 
                `orders_shipping_information_middle_name`, 
                `orders_shipping_information_last_name`, 
                `orders_shipping_information_company`, 
                `orders_shipping_information_phone`, 
                `orders_shipping_information_street1`, 
                `orders_shipping_information_street2`, 
                `orders_shipping_information_city`, 
                `orders_shipping_information_state`, 
                `orders_shipping_information_country`, 
                `orders_refund_total`, 
                `orders_allow_payment`, 
                `orders_allow_paypal`, 
                `orders_order_items_name`, 
                `orders_order_items_description`, 
                `orders_order_items_type`, 
                `orders_order_items_notes`, 
                `orders_order_items_cost`, 
                `orders_order_items_price`, 
                `orders_order_items_discount`, 
                `orders_order_items_specialAmount`, 
                `orders_order_items_product_name`, 
                `orders_order_items_product_sku`, 
                `orders_order_items_product_description`, 
                `orders_order_items_subscriptionPlan`, 
                `orders_payment_plan`, 
                cast(transaction_date as datetime) as transaction_date, 
                `gateway_account_name`, 
                `collection_method`, 
                `test`, 
                `orders_shipping_information_invoiceToCompany`, 
                `orders_order_items_product_shippable`, 
                `orders_order_items_product_taxable`, 
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 



def userInfo():
    try:
        args= getResolvedOptions(sys.argv, ['User_source_table','User_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('User_source_table')
        target_table=args.get('User_target_table')
        mySql_insert_query = """ replace INTO {}
                (`sub`, 
                `global_user_id`, 
                `email`, 
                `given_name`, 
                `family_name`, 
                `middle_name`, 
                `infusionsoft_id`, 
                `preferred_name`,
                `upd_user`,
                `upd_date`)
                select
                `sub`, 
                `global_user_id`, 
                `email`, 
                `given_name`, 
                `family_name`, 
                `middle_name`, 
                `infusionsoft_id`, 
                `preferred_name`,
                "pdl" as `upd_user`,
                current_timestamp() as `upd_date`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {} table".format(target_table))
        cursor.close()

    except mysql.connector.Error as error:
        print("Failed to insert record into {} table {}".format(error,target_table))

    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed") 


def staging_to_publish_load():
  contacts()
  countries()
  files()
  merchants()
  opportunities()
  orders()
  products()
  subscriptions()
  tags()
  transactions()
  userInfo()
  print("glue jobe name="+name)



staging_to_publish_load()