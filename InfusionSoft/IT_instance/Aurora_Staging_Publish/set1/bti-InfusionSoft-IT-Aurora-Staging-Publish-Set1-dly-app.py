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
                (`address_postal_code`, 
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
                 `business_secondary_color`,
				 `upd_user`,
				 `upd_date`)
                select 
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
                `business_secondary_color`,
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


def affiliatePrograms():
    try:
        args= getResolvedOptions(sys.argv, ['Aff_Pro_source_table','Aff_Pro_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Aff_Pro_source_table')
        target_table=args.get('Aff_Pro_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
                `priority`, 
                `affiliate_id`, 
                `name`, 
                `notes`,
                `upd_user`,
                `upd_date`)
                select 
                `id`, 
                `priority`, 
                `affiliate_id`, 
                `name`, 
                `notes`, 
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


def affiliateRedirects():
    try:
        args= getResolvedOptions(sys.argv, ['Aff_Redi_source_table','Aff_Redi_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Aff_Redi_source_table')
        target_table=args.get('Aff_Redi_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
                `affiliate_id`, 
                `name`, 
                `local_url_code`, 
                `redirect_url`, 
                `program_ids`,
                `upd_user`,
                `upd_date`)
                select
                `id`, 
                `affiliate_id`, 
                `name`, 
                `local_url_code`, 
                `redirect_url`, 
                `program_ids`,
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


def affiliates():
    try:
        args= getResolvedOptions(sys.argv, ['Aff_source_table','Aff_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Aff_source_table')
        target_table=args.get('Aff_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
                `contact_id`, 
                `parent_id`, 
                `track_leads_for`, 
                `code`, 
                `name`, 
                `status`, 
                `notify_on_lead`, 
                `notify_on_sale`,
                `upd_user`,
                `upd_date`)
                select
                `id`, 
                `contact_id`, 
                `parent_id`, 
                `track_leads_for`, 
                `code`, 
                `name`, 
                `status`, 
                `notify_on_lead`, 
                `notify_on_sale`,
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


def campaigns():
    try:
        args= getResolvedOptions(sys.argv, ['Camp_source_table','Camp_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Camp_source_table')
        target_table=args.get('Camp_target_table')
        mySql_insert_query = """ replace INTO {}
                (`active_contact_count`, 
                `id`, 
                `completed_contact_count`, 
                `error_message`, 
                `date_created`, 
                `name`, 
                `published_time_zone`, 
                `time_zone`, 
                `published_date`, 
                `published_status`,
                `upd_user`,
                `upd_date`)
                select
                `active_contact_count`, 
                `id`, 
                `completed_contact_count`, 
                `error_message`, 
                `date_created`, 
                `name`, 
                `published_time_zone`, 
                `time_zone`, 
                `published_date`, 
                `published_status`, 
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


def companies():
    try:
        args= getResolvedOptions(sys.argv, ['Comp_source_table','Comp_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Comp_source_table')
        target_table=args.get('Comp_target_table')
        mySql_insert_query = """ replace INTO {}
                (`id`, 
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
                `email_opted_in`,
                `upd_user`,
                `upd_date`)
                select
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
                `email_opted_in`,  
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
   accountProfile()
   affiliatePrograms()
   affiliateRedirects()
   affiliates()
   campaigns()
   companies()
   print("glue jobe name="+name)



staging_to_publish_load()