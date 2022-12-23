import mysql.connector
import sys
from awsglue.utils import getResolvedOptions
import boto3
args= getResolvedOptions(sys.argv, ['job_name'])
name=args.get('job_name')
def bti_Facebook():
    try:
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
        
        args= getResolvedOptions(sys.argv, ['Source_table','Target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Source_table')
        target_table=args.get('Target_table')
        mySql_insert_query = """INSERT INTO {}
                (`Impressions`, 
                `Day`, 
                `Ad_Name`, 
                `Account_ID`, 
                `Account_Name`, 
                `Ad_ID`, 
                `Ad_Set_ID`, 
                `Ad_Set_Name`, 
                `Campaign_ID`, 
                `Campaign_Name`, 
                `Link_Clicks`, 
                `Currency`, 
                `Amount_Spent__USD`, 
                `Reporting_Starts`, 
                `Reporting_Ends`,
                `upd_user`,
                `upd_date`)
                select 
                fs.`Impressions`, 
                cast(fs.Day as Date) as Day, 
                fs.`Ad_Name`, 
                fs.`Account_ID`, 
                fs.`Account_Name`, 
                fs.`Ad_ID`, 
                fs.`Ad_Set_ID`, 
                fs.`Ad_Set_Name`, 
                fs.`Campaign_ID`, 
                fs.`Campaign_Name`, 
                fs.`Link_Clicks`, 
                fs.`Currency`, 
                fs.`Amount_Spent__USD`, 
                cast(fs.Reporting_Starts as Date) as Reporting_Starts, 
                cast(fs.Reporting_Ends as Date) as Reporting_Ends,
                "pdl" as "upd_user",
                current_timestamp() as "upd_date"
                from {} as fs
                 left outer join
				{} as fp
				on 
				fs.`Day` = fp.`Day`
				where fp.`Day`  is NULL""".format(target_table,source_table,target_table)
        cursor = connection.cursor()
        cursor.execute(mySql_insert_query)
        connection.commit()
        print(cursor.rowcount, "Record inserted successfully into {}".format(target_table))
        cursor.close()
    
    except mysql.connector.Error as error:
        print("Failed to insert record into {}".format(error,target_table))
    
    finally:
        if connection.is_connected():
            connection.close()
            print("MySQL connection is closed")

def bti_Facebook_Aurora_Publish_dly_app():
    bti_Facebook()
    print("glue jobe name="+name)
bti_Facebook_Aurora_Publish_dly_app()

