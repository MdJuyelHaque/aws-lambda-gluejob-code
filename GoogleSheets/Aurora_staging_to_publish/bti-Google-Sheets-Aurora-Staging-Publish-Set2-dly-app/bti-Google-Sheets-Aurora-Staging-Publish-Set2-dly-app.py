import mysql.connector
import sys
import boto3
from awsglue.utils import getResolvedOptions
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

def DEC_Goals():
    try:
        args= getResolvedOptions(sys.argv, ['Dec_source_table','Dec_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Dec_source_table')
        target_table=args.get('Dec_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
        INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `Goal_of_Calls`,
            `Goal_of_DBL_Tickets`,
            `Goal_of_VIP_Upg`,
            `Timestamp`,
            `Email_Address`,
            `Type_of_Goal`,
            `Date`,
            `Goal_of_Conversations`,
            `Goal_of_Friends__Family_Tickets`,
            `Goal_of_Deep_Dive_Training_Tickets`,
            `Goal_of_VIP_Day_Tickets`)
            select 
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`Goal_of_Calls`,
            	`Goal_of_DBL_Tickets`,
            	`Goal_of_VIP_Upg`,
            	STR_TO_DATE(`Timestamp`, '%m/%d/%Y %H:%i:%S') as `Timestamp`,
            	`Email_Address`,
            	`Type_of_Goal`,
            	STR_TO_DATE(`Date`, '%m/%d/%Y') as `Date`,
            	`Goal_of_Conversations`,
            	`Goal_of_Friends__Family_Tickets`,
            	`Goal_of_Deep_Dive_Training_Tickets`,
            	`Goal_of_VIP_Day_Tickets`
            from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def energy_board():
    try:
        args= getResolvedOptions(sys.argv, ['energy_source_table','energy_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('energy_source_table')
        target_table=args.get('energy_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
        INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `Client_ID`,
            `SS_Date`,
            `Client_name`,
            `Phone_Number`,
            `Zoom_link`,
            `Opp_Lead_Type`,
            `CC`,
            `PE`,
            `FM`,
            `Status`,
            `Booked_Type`,
            `Platform`,
            `type_of_lead`,
            `PAID_or_ORGANIC`,
            `TEAM`,
            `DIVISION`,
            `date`,
            `period`,
            `trim_spaces`,
            `CALENDLY`)
            select 
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`Client_ID`,
            	str_to_date(`SS_Date`,'%d-%M-%Y') as `SS_Date`,
            	`Client_name`,
            	`Phone_Number`,
            	`Zoom_link`,
            	`Opp_Lead_Type`,
            	`CC`,
            	`PE`,
            	`FM`,
            	`Status`,
            	`Booked_Type`,
            	`Platform`,
            	`type_of_lead`,
            	`PAID_or_ORGANIC`,
            	`TEAM`,
            	`DIVISION`,
            	str_to_date(`date`,'%d-%M-%Y') as `date`,
            	`period`,
            	`trim_spaces`,
            	`CALENDLY`
            from {};""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def FrontLine_Goals():
    try:
        args= getResolvedOptions(sys.argv, ['FG_source_table','FG_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('FG_source_table')
        target_table=args.get('FG_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
         INSERT INTO {}
                (`dest_upd_user`,
                `dest_upd_date`,
                `Strategy_Session_Set`,
                `Timestamp`,
                `Email_Address`,
                `Type_of_Goal`,
                `Date`,
                `Amount_of_Calls`,
                `Conversations`,
                `Quantity_of_Sales_`,
                `Volume_of_Sales`)
                select
                	"pdl" as `dest_upd_user`,
                	current_timestamp() as `dest_upd_date`,
                	`Strategy_Session_Set`,
                	`Timestamp`,
                	`Email_Address`,
                	`Type_of_Goal`,
                	str_to_date(`Date`,'%m/%d/%Y')`Date`,
                	`Amount_of_Calls`,
                	`Conversations`,
                	`Quantity_of_Sales_`,
                	`Volume_of_Sales`
                from {}""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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
            
def FrontLine_SS_Set():
    try:
        args= getResolvedOptions(sys.argv, ['FSS_source_table','FSS_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('FSS_source_table')
        target_table=args.get('FSS_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
            INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `SS_SET`,
            `Timestamp`,
            `Email_Address`,
            `Todays_Date`)
            select 
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`SS_SET`,
            	`Timestamp`,
            	`Email_Address`,
            	str_to_date(`Todays_Date`,'%m/%d/%Y') as `Todays_Date`
            from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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
            

def Funnel_Goals():
    try:
        args= getResolvedOptions(sys.argv, ['Funnel_G_Source_table','Funnel_G_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('Funnel_G_Source_table')
        target_table=args.get('Funnel_G_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
        INSERT INTO {}
                (`dest_upd_user`,
                `dest_upd_date`,
                `Month`,
                `Funnel_Status`,
                `Funnel_Status_Type`,
                `Funnel_Status_SubType`,
                `OptIns`,
                `Marketing_Sales`,
                `Leads`,
                `Strategy_Sessions`,
                `Enrollment_Sales`,
                `UPD_DT`,
                `UPD_User`)
                select
                	"pdl" as `dest_upd_user`,
                	current_timestamp() as `dest_upd_date`,
                	str_to_date(`Month`,'%m/%d/%Y') as `Month`,
                	`Funnel_Status`,
                	`Funnel_Status_Type`,
                	`Funnel_Status_SubType`,
                	`OptIns`,
                	`Marketing_Sales`,
                	`Leads`,
                	`Strategy_Sessions`,
                	`Enrollment_Sales`,
                	 `UPD_DT`,
                	`UPD_User`
                from {};""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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


def Funnel_Status_Lookup():
    try:
        args= getResolvedOptions(sys.argv, ['FSL_Source_table','FSL_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('FSL_Source_table')
        target_table=args.get('FSL_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
                INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `Funel_Promo_plus_Lead_Data_Source`,
            `Funnel_Status`,
            `Funnel_Status_Type`,
            `Funnel_Status_SubType`,
            `UPD_DT`,
            `UPD_User`)
            select
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`Funel_Promo_plus_Lead_Data_Source`,
            	`Funnel_Status`,
            	`Funnel_Status_Type`,
            	`Funnel_Status_SubType`,
            	`UPD_DT`,
            	`UPD_User`
            from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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
            
def InHouse_Goals():
    try:
        args= getResolvedOptions(sys.argv, ['IHG_Source_table','IHG_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('IHG_Source_table')
        target_table=args.get('IHG_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
        INSERT INTO {}
                (`dest_upd_user`,
                `dest_upd_date`,
                `Amount_of_Calls`,
                `Strategy_Sessions_Set`,
                `Quantity_of_Sales`,
                `Volume_Of_Sales`,
                `Timestamp`,
                `Type_of_Goal`,
                `Date`,
                `Amount_of_Conversations`,
                `Email_Address`)
                select 
                	"pdl" as `dest_upd_user`,
                	current_timestamp() as `dest_upd_date`,
                	`Amount_of_Calls`,
                	`Strategy_Sessions_Set`,
                	`Quantity_of_Sales`,
                	`Volume_Of_Sales`,
                	STR_TO_DATE(`Timestamp`, '%m/%d/%Y %H:%i:%S') as `Timestamp`,
                	`Type_of_Goal`,
                	STR_TO_DATE(`Date`, '%m/%d/%Y') as `Date`,
                	`Amount_of_Conversations`,
                	`Email_Address`
                from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def InHouse_SS_Set():
    try:
        args= getResolvedOptions(sys.argv, ['IHSS_Source_table','IHSS_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('IHSS_Source_table')
        target_table=args.get('IHSS_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
        INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `Amount_of_Strategy_Sessions_Set_Today`,
            `Timestamp`,
            `Todays_Date`,
            `Email_Address`)
            select 
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`Amount_of_Strategy_Sessions_Set_Today`,
            	STR_TO_DATE(`Timestamp`, '%m/%d/%Y %H:%i:%S') as `Timestamp`,
            	STR_TO_DATE(`Todays_Date`, '%m/%d/%Y') as `Todays_Date`,
            	`Email_Address`
            from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def Lead_Source_Data_Lookup():
    try:
        args= getResolvedOptions(sys.argv, ['LSDL_Source_table','LSDL_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('LSDL_Source_table')
        target_table=args.get('LSDL_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
            INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `Lead_Source_Data`,
            `Lead_Source_Data_Type`,
            `UPD_DT`,
            `UPD_User`)
            select
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`Lead_Source_Data`,
            	`Lead_Source_Data_Type`,
            	`UPD_DT`,
            	`UPD_User`
            from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def PAD_Goals():
    try:
        args= getResolvedOptions(sys.argv, ['PG_Source_table','PG_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('PG_Source_table')
        target_table=args.get('PG_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
        INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `Amount_of_Calls`,
            `Strategy_Sessions_Set`,
            `Quantity_of_Sales`,
            `Volume_Of_Sales`,
            `Timestamp`,
            `Email_Address`,
            `Type_of_Goal`,
            `Date`)
            select
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`Amount_of_Calls`,
            	`Strategy_Sessions_Set`,
            	`Quantity_of_Sales`,
            	`Volume_Of_Sales`,
            	STR_TO_DATE(`Timestamp`, '%m/%d/%Y %H:%i:%S') as `Timestamp`,
            	`Email_Address`,
            	`Type_of_Goal`,
            	STR_TO_DATE(`Date`, '%m/%d/%Y') as `Date`
            from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def PE_Goals():
    try:
        args= getResolvedOptions(sys.argv, ['PEG_Source_table','PEG_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('PEG_Source_table')
        target_table=args.get('PEG_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
            INSERT INTO {}
                (`dest_upd_user`,
                `dest_upd_date`,
                `Strategy_Sessions_Completed`,
                `Volume_of_Sales`,
                `Quantity_of_Sales`,
                `Timestamp`,
                `Email_Address`,
                `Type_of_Goal`,
                `Date`)
                select 
                	"pdl" as `dest_upd_user`,
                	current_timestamp() as `dest_upd_date`,
                	`Strategy_Sessions_Completed`,
                	`Volume_of_Sales`,
                	`Quantity_of_Sales`,
                	STR_TO_DATE(`Timestamp`, '%m/%d/%Y %H:%i:%S') as `Timestamp`,
                	`Email_Address`,
                	`Type_of_Goal`,
                	STR_TO_DATE(`Date`, '%m/%d/%Y') as `Date`
                from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def PAD_SS_Set():
    try:
        args= getResolvedOptions(sys.argv, ['PSS_Source_table','PSS_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('PSS_Source_table')
        target_table=args.get('PSS_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
                INSERT INTO {}
        (`dest_upd_user`,
        `dest_upd_date`,
        `Strategy_Sessions_Set`,
        `Timestamp`,
        `Email_Address`,
        `DATE`)
        select
            "pdl" as `dest_upd_user`,
        	current_timestamp() as `dest_upd_date`,
        	`Strategy_Sessions_Set`,
        	STR_TO_DATE(`Timestamp`, '%m/%d/%Y %H:%i:%S') as `Timestamp`,
        	`Email_Address`,
        	STR_TO_DATE(`DATE`, '%m/%d/%Y') as `DATE`
        from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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


def Referral_Partner_Goals():
    try:
        args= getResolvedOptions(sys.argv, ['RPG_Source_table','RPG_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('RPG_Source_table')
        target_table=args.get('RPG_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
            INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `Month`,
            `Referral_Partner_Category`,
            `SKU_Category`,
            `Funnel`,
            `Unit_Sales_GOAL`,
            `Sales_Volume_GOAL`,
            `Unit_Sales_BUDGET`,
            `Sales_Volume_BUDGET`,
            `UPD_DT`,
            `UPD_User`)
            select 
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`Month`,
            	`Referral_Partner_Category`,
            	`SKU_Category`,
            	`Funnel`,
            	`Unit_Sales_GOAL`,
            	`Sales_Volume_GOAL`,
            	`Unit_Sales_BUDGET`,
            	`Sales_Volume_BUDGET`,
            	`UPD_DT`,
            	`UPD_User`
            from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def Products_Categories():
    try:
        args= getResolvedOptions(sys.argv, ['PC_Source_table','PC_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('PC_Source_table')
        target_table=args.get('PC_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
                INSERT INTO {}
                    (`dest_upd_user`,
                    `dest_upd_date`,
                    `Id`,
                    `Products`,
                    `Sku_Category2`,
                    `Sku_Category`,
                    `Funnel_Promo`,
                    `Sku`,
                    `UPD_DT`,
                    `UPD_User`,
                    `Division`,
                    `EBP_NEBP`)
                    select
                    	"pdl" as `dest_upd_user`,
                        current_timestamp() as `dest_upd_date`,
                    	`Id`,
                    	`Products`,
                    	`Sku_Category2`,
                    	`Sku_Category`,
                    	`Funnel_Promo`,
                    	`Sku`,
                    	`UPD_DT`,
                    	`UPD_User`,
                    	`Division`,
                    	`EBP_NEBP`
                    from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def Referral_Partner_Lookup():
    try:
        args= getResolvedOptions(sys.argv, ['RPL_Source_table','RPL_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('RPL_Source_table')
        target_table=args.get('RPL_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
            INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `Id`,
            `Referral_Partner`,
            `Referral_Partner_Type`,
            `Enrollment_Mentor`,
            `Appt_Setter`,
            `Team_Name`,
            `Floor_Manager`,
            `UPD_DT`,
            `UPD_User`)
            select 
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`Id`,
            	`Referral_Partner`,
            	`Referral_Partner_Type`,
            	`Enrollment_Mentor`,
            	`Appt_Setter`,
            	`Team_Name`,
            	`Floor_Manager`,
            	`UPD_DT`,
            	`UPD_User`
            from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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

def Enrollment_Team_Names():
    try:
        args= getResolvedOptions(sys.argv, ['ETN_Source_table','ETN_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('ETN_Source_table')
        target_table=args.get('ETN_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
            INSERT INTO {}
            (`dest_upd_user`,
            `dest_upd_date`,
            `Id`,
            `Reporting_Name`,
            `POSITION`,
            `TEAM`,
            `Active_or_Former_Employee`,
            `email`,
            `Note`,
            `UPD_DT`,
            `UPD_User`)
            SELECT 
            	"pdl" as `dest_upd_user`,
            	current_timestamp() as `dest_upd_date`,
            	`Id`,
            	`Reporting_Name`,
            	`POSITION`,
            	`TEAM`,
            	`Active_or_Former_Employee`,
            	`email`,
            	`Note`,
            	str_to_date(UPD_DT,'%m/%d/%Y')`UPD_DT`,
            	`UPD_User`
            FROM {};""".format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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


def Campaign_Performance_Funnels():
    try:
        args= getResolvedOptions(sys.argv, ['CPF_Source_table','CPF_target_table','database','port'])
        database=args.get('database')
        port=args.get('port')
        connection = mysql.connector.connect(host="{}".format(host),port="{}".format(port),user="{}".format(user),password="{}".format(password),database="{}".format(database))
        source_table=args.get('CPF_Source_table')
        target_table=args.get('CPF_target_table')
        mysql_truncate_query = """truncate {}""".format(target_table)
        mySql_insert_query = """
            INSERT INTO {}
                (`id`,
                `Funnel_Status`,
                `UPD_DT`,
                `UPD_User`,
                `dest_upd_user`,
                `dest_upd_date`)
                select
                	`id`,
                	`Funnel_Status`,
                	`UPD_DT`,
                	`UPD_User`,
                	"pdl" as `dest_upd_user`,
                	current_timestamp() as `dest_upd_date`
                from {} """.format(target_table,source_table)
        cursor = connection.cursor()
        cursor.execute(mysql_truncate_query)
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
    DEC_Goals()
    energy_board()
    FrontLine_Goals()
    FrontLine_SS_Set()
    Funnel_Goals()
    Funnel_Status_Lookup()
    InHouse_Goals()
    InHouse_SS_Set()
    Lead_Source_Data_Lookup()
    PAD_Goals()
    PE_Goals()
    PAD_SS_Set()
    Referral_Partner_Goals()
    Products_Categories()
    Referral_Partner_Lookup()
    Enrollment_Team_Names()
    Campaign_Performance_Funnels()
    print("glue jobe name="+name)
        
staging_to_publish_load()
