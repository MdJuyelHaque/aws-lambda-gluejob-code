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


def accountProfile():
    try:
        args= getResolvedOptions(sys.argv, ['Acc_Pro_source_table','Acc_Pro_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Acc_Pro_source_table')
        target_table=args.get('Acc_Pro_target_table')
        mySql_insert_query = """ replace INTO {}
                (`upd_date`,
                 `upd_user`,
                 `address_postal_code`, 
                 `address_zip_code`, 
                 `name`, 
                 `email`, 
                 `website`, 
                 `phone`, 
                 `address_line1`, 
                 `address_line2`, 
                 `address_locality`, 
                 `address_region`, 
                 `address_field`, 
                 `address_zip_four`, 
                 `address_country_code`, 
                 `phone_ext`, 
                 `time_zone`, 
                 `logo_url`, 
                 `currency_code`, 
                 `language_tag`, 
                 `business_type`, 
                 `business_primary_color`, 
                 `business_secondary_color`)
                select 
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`,
                `address_postal_code`, 
                `address_zip_code`, 
                `name`, 
                `email`, 
                `website`, 
                `phone`, 
                `address_line1`, 
                `address_line2`, 
                `address_locality`, 
                `address_region`, 
                `address_field`, 
                `address_zip_four`, 
                `address_country_code`, 
                `phone_ext`, 
                `time_zone`, 
                `logo_url`, 
                `currency_code`, 
                `language_tag`, 
                `business_type`, 
                `business_primary_color`, 
                `business_secondary_color`
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


def affiliateCommissions():
    try:
        args= getResolvedOptions(sys.argv, ['Aff_Com_source_table','Aff_Com_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Aff_Com_source_table')
        target_table=args.get('Aff_Com_target_table')
        mySql_insert_query = """ replace INTO {}
                (`upd_date`,
                `upd_user`,
                `invoice_id`,
                `contact_id`, 
                `sales_affiliate_id`, 
                `amount_earned`, 
                `description`, 
                `contact_first_name`, 
                `contact_last_name`, 
                `date_earned`, 
                `sold_by_first_name`,
                `sold_by_last_name`, 
                `product_name`)
                select 
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`,
                `invoice_id`,
                `contact_id`, 
                `sales_affiliate_id`, 
                `amount_earned`, 
                `description`, 
                `contact_first_name`, 
                `contact_last_name`,
                cast(date_earned as datetime) as date_earned,  
                `sold_by_first_name`,
                `sold_by_last_name`, 
                `product_name`
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


def affiliateModel():
    try:
        args= getResolvedOptions(sys.argv, ['Aff_Mod_source_table','Aff_Mod_target_table','database','port'])
    
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Aff_Mod_source_table')
        target_table=args.get('Aff_Mod_target_table')
        mySql_insert_query = """ replace INTO {}
                (`upd_date`,
                `upd_user`,
                `id`, 
                `label`,  
                `record_type`, 
                `field_type`, 
                `field_name`)
                select 
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`,
                `id`, 
                `label`,  
                `record_type`, 
                `field_type`, 
                `field_name`
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



def affiliatePrograms():
    try:
        args= getResolvedOptions(sys.argv, ['Aff_Pro_source_table','Aff_Pro_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Aff_Pro_source_table')
        target_table=args.get('Aff_Pro_target_table')
        mySql_insert_query = """ replace INTO {}
                (`upd_date`,
                `upd_user`,
                `id`, 
                `priority`, 
                `affiliate_id`, 
                `name`, 
                `notes`)
                select
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`, 
                `id`, 
                `priority`, 
                `affiliate_id`, 
                `name`, 
                `notes`
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


def affiliateRedirects():
    try:
        args= getResolvedOptions(sys.argv, ['Aff_Redi_source_table','Aff_Redi_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Aff_Redi_source_table')
        target_table=args.get('Aff_Redi_target_table')
        mySql_insert_query = """ replace INTO {}
                (`upd_date`,
                `upd_user`,
                `id`, 
                `affiliate_id`, 
                `name`, 
                `local_url_code`, 
                `redirect_url`, 
                `program_ids`)
                select
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`,
                `id`, 
                `affiliate_id`, 
                `name`, 
                `local_url_code`, 
                `redirect_url`, 
                `program_ids`
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


def affiliates():
    try:
        args= getResolvedOptions(sys.argv, ['Aff_source_table','Aff_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Aff_source_table')
        target_table=args.get('Aff_target_table')
        mySql_insert_query = """ replace INTO {}
                (`upd_date`,
                `upd_user`,
                `id`, 
                `contact_id`, 
                `parent_id`, 
                `track_leads_for`, 
                `code`, 
                `name`, 
                `status`, 
                `notify_on_lead`, 
                `notify_on_sale`)
                select
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`,
                `id`, 
                `contact_id`, 
                `parent_id`, 
                `track_leads_for`, 
                `code`, 
                `name`, 
                `status`, 
                `notify_on_lead`, 
                `notify_on_sale`
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


def campaigns():
    try:
        args= getResolvedOptions(sys.argv, ['Camp_source_table','Camp_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Camp_source_table')
        target_table=args.get('Camp_target_table')
        mySql_insert_query = """ replace INTO {}
                (`upd_date`,
                `upd_user`,
                `active_contact_count`, 
                `id`, 
                `completed_contact_count`, 
                `error_message`, 
                `date_created`, 
                `name`, 
                `published_time_zone`, 
                `time_zone`, 
                `published_date`, 
                `published_status`)
                select
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`,
                `active_contact_count`, 
                `id`, 
                `completed_contact_count`, 
                `error_message`, 
                cast(date_created as datetime) as date_created, 
                `name`, 
                `published_time_zone`, 
                `time_zone`,
                cast(published_date as datetime) as published_date, 
                `published_status`
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


def companies():
    try:
        args= getResolvedOptions(sys.argv, ['Comp_source_table','Comp_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Comp_source_table')
        target_table=args.get('Comp_target_table')
        mySql_insert_query = """ replace INTO {}
                (`upd_date`,
                `upd_user`,
                `id`, 
                `website`, 
                `address_line1`, 
                `address_line2`, 
                `address_locality`, 
                `address_region`, 
                `address_zip_code`, 
                `address_zip_four`, 
                `address_country_code`, 
                `email_address`, 
                `company_name`, 
                `phone_number_number`, 
                `phone_number_extension`, 
                `phone_number_type`, 
                `email_status`, 
                `email_opted_in`)
                select
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`,
                `id`, 
                `website`, 
                `address_line1`, 
                `address_line2`, 
                `address_locality`, 
                `address_region`, 
                `address_zip_code`, 
                `address_zip_four`, 
                `address_country_code`, 
                `email_address`, 
                `company_name`, 
                `phone_number_number`, 
                `phone_number_extension`, 
                `phone_number_type`, 
                `email_status`, 
                `email_opted_in`
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
   accountProfile()
   affiliateCommissions()
   affiliateModel()
   affiliatePrograms()
   affiliateRedirects()
   affiliates()
   campaigns()
   companies()
   print("glue jobe name="+name)



staging_to_publish_load()

