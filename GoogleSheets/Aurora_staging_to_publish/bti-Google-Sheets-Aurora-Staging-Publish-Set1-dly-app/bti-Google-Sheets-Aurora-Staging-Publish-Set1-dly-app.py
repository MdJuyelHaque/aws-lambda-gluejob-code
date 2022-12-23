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


def Teams_Sergey_Format():
    try:
        
        args= getResolvedOptions(sys.argv, ['TSF_source_table','TSF_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('TSF_source_table')
        target_table=args.get('TSF_target_table')
        mySql_insert_query = """ replace INTO {}
                (`Team`,
                `Opp_Lead_Type`,
                `Category`,
                `Sub_Category`,
                `Id`,
                `UPD_DT`,
                `UPD_User`,
                `dest_upd_user`,
                `dest_upd_date`)
                select 
                	`Team`,
                	`Opp_Lead_Type`,
                	`Category`,
                	`Sub_Category`,
                	`Id`,
                	`UPD_DT`,
                	`UPD_User`,
                	"pdl" as `dest_upd_user`,
                	current_timestamp() as `dest_upd_date`
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
            
            
def ThreeCLogic_Names():
    try:
        args= getResolvedOptions(sys.argv, ['3C_Names_source_table','3C_Names_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('3C_Names_source_table')
        target_table=args.get('3C_Names_target_table')
        mySql_insert_query = """
                replace INTO {}
                (`dest_upd_user`,
                `dest_upd_date`,
                `Id`,
                `Member`,
                `Enrollment_Mentor`,
                `UPD_DT`,
                `UPD_User`)
                select 
                	"pdl" as `dest_upd_user`,
                	current_timestamp() as `dest_upd_date`,
                	`Id`,
                	`Member`,
                	`Enrollment_Mentor`,
                	`UPD_DT`,
                	`UPD_User`
                from  {} """.format(target_table,source_table)
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
   Teams_Sergey_Format()
   ThreeCLogic_Names()
   print("glue jobe name="+name)

staging_to_publish_load()

