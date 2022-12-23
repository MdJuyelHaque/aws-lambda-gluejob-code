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

def All_Sales_17002():
    try:
         
        args= getResolvedOptions(sys.argv, ['All_Sales_source_table','All_Sales_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('All_Sales_source_table')
        target_table=args.get('All_Sales_target_table')
        mySql_insert_query = """ INSERT INTO {}
            (`InvoiceId`,
            `ContactId`,
            `Name`,
            `ReferralPartner`,
            `Products`,
            `InvTotal`,
            `Date`,
            `Custom_LeadSourceData0`,
            `Custom_OrderID`,
            `upd_user`,
            `upd_date`)
            select 
            	`InvoiceId`,
            	`ContactId`,
            	`Name`,
            	`ReferralPartner`,
            	`Products`,
            	`InvTotal`,
            	`Date`,
            	`Custom_LeadSourceData0`,
            	`Custom_OrderID`,
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
            


def CGC_17010():
    try:
        args= getResolvedOptions(sys.argv, ['CGC_source_table','CGC_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('CGC_source_table')
        target_table=args.get('CGC_target_table')
        mySql_insert_query = """ INSERT INTO {}
            (`Id`,
            `ContactId`,
            `ContactName`,
            `funnel_goalachieved_funnel_name`,
            `funnel_goalachieved_goal_name`,
            `funnel_goalachieved_date_achieved`,
            `Email`,
            `Custom_LeadSourceData0`,
            `upd_user`,
            `upd_date`)
            select
            	`Id`,
            	`ContactId`,
            	`ContactName`,
            	`funnel_goalachieved_funnel_name`,
            	`funnel_goalachieved_goal_name`,
            	`funnel_goalachieved_date_achieved`,
            	`Email`,
            	`Custom_LeadSourceData0`,
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
  

def Payments_17012():
    try:
       
        args= getResolvedOptions(sys.argv, ['Payments_17012_source_table','Payments_17012_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Payments_17012_source_table')
        target_table=args.get('Payments_17012_target_table')
        mySql_insert_query = """ INSERT INTO {}
            (`Id`,
            `ContactId`,
            `SaleTotal`,
            `PayAmt`,
            `Name`,
            `ReferralPartner`,
            `ProductNames`,
            `PayType`,
            `Date`,
            `upd_user`,
            `upd_date`)
            select
            	`Id`,
                `ContactId`,
                `SaleTotal`,
                `PayAmt`,
                `Name`,
                `ReferralPartner`,
                `ProductNames`,
                `PayType`,
                `Date`,
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
            
            
def Data_Lake_Orders_17312():
    try:
        
        args= getResolvedOptions(sys.argv, ['Data_Lake_Orders_17312_source_table','Data_Lake_Orders_17312_target_table','database','port'])
        
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Data_Lake_Orders_17312_source_table')
        target_table=args.get('Data_Lake_Orders_17312_target_table')
        mySql_insert_query = """ INSERT INTO {}
            (InvoiceId, 
			OrderDate, 
			Job_PreviousOrderSKU, 
			Job_PreviousOrderInvoice, 
			Job_ReasonforCreditandRebill, 
            `upd_user`,
            `upd_date`)
            select
            	InvoiceId, 
				OrderDate, 
				Job_PreviousOrderSKU, 
				Job_PreviousOrderInvoice, 
				Job_ReasonforCreditandRebill,
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
   print("All_Sales Started")
   All_Sales_17002()
   print("All_Sales_17002 Ended")
   print("Campaign Goal Completion Started")
   CGC_17010()
   print("Campaign Goal Completion Ended")
   print("Payments Started")
   Payments_17012()
   print("Payments Ended")
   print("glue jobe name="+name)
   print("Data_Lake_Orders Started")
   Data_Lake_Orders_17312()
   print("Data_Lake_Orders Ended")
   print("glue jobe name="+name)



staging_to_publish_load()

