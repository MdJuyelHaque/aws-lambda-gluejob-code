import mysql.connector
import sys
from awsglue.utils import getResolvedOptions
import boto3
args= getResolvedOptions(sys.argv, ['job_name'])   
name=args.get('job_name')
def bti_3clogic():
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
                (`Project`,
                `Member`,
                `Flow_Start_Time`,
                `Interaction_Type`,
                `Remote_Party`,
                `Local_Party`,
                `Agent_Ring_Duration`,
                `Preview_Time`,
                `Handle_Time`,
                `Talk_Time`,
                `Hold_Time`,
                `Wrapup_Time`,
                `Transfer_Talk_Time`,
                `Email_Subject`,
                `Email_Response_Time`,
                `Email_Handle_Time`,
                `Disposition_Service`,
                `Connection_Duration_Seconds`,
                `Abandoned_Dialer`,
                `Transfer_Mode`,
                `Transfer_Type`,
                `Transfer_Status`,
                `Ext_CRMID`,
                `CRMID`,
                `Disposition`,
                `Flow_Type`,
                `Urgent`,
                `Scheduled`,
                `Local_Hangup`,
                `System_Disposition`,
                `System_Disposition_Reason`,
                `SIP_RCode`,
                `Email`,
                `Audio`,
                `Audio_Internal`,
                `Screen_Recording`,
                `Screen_Capture`,
                `Dialer_Mode`,
                `Dialer_Time`,
                `Dialer_Ring_Duration`,
                `Agent_Dialing_Time`,
                `AMD`,
                `Interaction_Active_Duration`,
                `Interaction_Passive_Duration`,
                `PDD_Duration`,
                `FAX`,
                `Abdndoned_IVR`,
                `IVR_Time`,
                `Queue_Time`,
                `Skill_Group_Name`,
                `Conference_Id`,
                `Conference_Duration`,
                `Abandoned_Queue`,
                `Entry_Position`,
                `V_Hold_Time`,
                `V_Hold`,
                `Business_Flow_Id`,
                `Call_Id`,
                `Transfer_Party`,
                `Notes`,
                `guid__Lead`,
                `olsentimezone__Lead`,
                `creationtime__Lead`,
                `firstName__Lead`,
                `lastName__Lead`,
                `mobilePhone__Lead`,
                `homePhone__Lead`,
                `workPhone__Lead`,
                `source__Lead`,
                `timezone__Lead`,
                `prefix__Lead`,
                `email__Lead`,
                `language__Lead`,
                `street__Lead`,
                `street1__Lead`,
                `street2__Lead`,
                `city__Lead`,
                `state__Lead`,
                `zipPostalCode__Lead`,
                `country__Lead`,
                `webSite__Lead`,
                `company__Lead`,
                `noOfEmployees__Lead`,
                `revenue__Lead`,
                `industry__Lead`,
                `requirements__Lead`,
                `contactID__Lead`,
                `opportunityID__Lead`,
                `upd_user`,
                `upd_date`)
                select Project,
                Member,
                STR_TO_DATE(Flow_Start_Time, '%m/%d/%Y %H:%i:%S') as Flow_Start_Time,
                Interaction_Type,
                Remote_Party,
                Local_Party,
                cast(Agent_Ring_Duration as Time) as Agent_Ring_Duration,
                cast(Preview_Time as Time) as Preview_Time,
                cast(Handle_Time as Time) as Handle_Time,
                cast(Talk_Time as Time) as Talk_Time,
                cast(Hold_Time as Time) as Hold_Time,
                cast(Wrapup_Time as Time) as Wrapup_Time,
                cast(Transfer_Talk_Time as Time) as Transfer_Talk_Time,
                Email_Subject,
                cast(Email_Response_Time as Time) as Email_Response_Time,
                cast(Email_Handle_Time as Time) as Email_Handle_Time,
                Disposition_Service,
                convert(Connection_Duration_Seconds,decimal(10,3)) as Connection_Duration_Seconds,
                Abandoned_Dialer,
                Transfer_Mode,
                Transfer_Type,
                Transfer_Status,
                Ext_CRMID,
                CRMID,
                Disposition,
                Flow_Type,
                Urgent,
                Scheduled,
                Local_Hangup,
                System_Disposition,
                System_Disposition_Reason,
                SIP_RCode,
                Email,
                Audio,
                Audio_Internal,
                Screen_Recording,
                Screen_Capture,
                Dialer_Mode,
                cast(Dialer_Time as Time) as Dialer_Time,
                cast(Dialer_Ring_Duration as Time) as Dialer_Ring_Duration,
                cast(Agent_Dialing_Time as Time) as Agent_Dialing_Time,
                AMD,
                cast(Interaction_Active_Duration as Time) as Interaction_Active_Duration,
                cast(Interaction_Passive_Duration as Time) as Interaction_Passive_Duration,
                cast(PDD_Duration as Time) as PDD_Duration,
                FAX,
                Abdndoned_IVR,
                cast(IVR_Time as Time) as IVR_Time,
                cast(Queue_Time as Time) as Queue_Time,
                Skill_Group_Name,
                Conference_Id,
                cast(Conference_Duration as Time) as Conference_Duration,
                Abandoned_Queue,
                Entry_Position,
                cast(V_Hold_Time as Time) as V_Hold_Time,
                V_Hold,
                Business_Flow_Id,
                Call_Id,
                Transfer_Party,
                Notes,
                guid__Lead,
                olsentimezone__Lead,
                creationtime__Lead,
                firstName__Lead,
                lastName__Lead,
                mobilePhone__Lead,
                homePhone__Lead,
                workPhone__Lead,
                source__Lead,
                timezone__Lead,
                prefix__Lead,
                email__Lead,
                language__Lead,
                street__Lead,
                street1__Lead,
                street2__Lead,
                city__Lead,
                state__Lead,
                zipPostalCode__Lead,
                country__Lead,
                webSite__Lead,
                company__Lead,
                noOfEmployees__Lead,
                revenue__Lead,
                industry__Lead,
                requirements__Lead,
                contactID__Lead,
                opportunityID__Lead,
                "pdl" as "upd_user",
                current_timestamp() as "upd_date"
                from {} where left(Flow_Start_Time,10) = `CurrDate`""".format(target_table,source_table)
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
def bti_3CLogic_Aurora_Publish_dly_app():
    bti_3clogic()
    print("glue jobe name="+name)
    
bti_3CLogic_Aurora_Publish_dly_app()
