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

            

def payments():
    try:
        args= getResolvedOptions(sys.argv, ['Pay_source_table','Pay_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Pay_source_table')
        target_table=args.get('Pay_target_table')
        mySql_insert_query = """ replace INTO {}
                (`upd_date`,
                `upd_user`,
                `Commission`, 
                `ContactId`, 
                `InvoiceId`, 
                `UserId`, 
                `Id`, 
                `RefundId`, 
                `ChargeId`, 
                `PayAmt`, 
                `PayType`, 
                `PayDate`, 
                `LastUpdated`,  
                `PayNote`,
                `Synced`)
                select
                current_timestamp() as `upd_date`,
                "pdl" as `upd_user`,
                `Commission`, 
                `ContactId`, 
                `InvoiceId`, 
                `UserId`, 
                `Id`, 
                `RefundId`, 
                `ChargeId`, 
                `PayAmt`, 
                `PayType`, 
                cast(PayDate as datetime) as PayDate,
                cast(LastUpdated as datetime) as LastUpdated,  
                `PayNote`,
                `Synced`
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
   payments()
   print("glue jobe name="+name)



staging_to_publish_load()
