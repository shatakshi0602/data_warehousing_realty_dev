

--- Time Travel in Past
SELECT * 
FROM TABLE
AT(OFFSET=>-60*60*12)

--- Create a procedure : sql template

CREATE OR REPLACE PROCEDURE TEST_PROCS_()
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
declare
qry1 varchar default 'SELECT CURRENT_DATE()';
begin
execute immediate :qry1;
return qry1;
end;
$$;

CALL TEST_PROCS_();


--- Create a procedure : sql+python template

CREATE OR REPLACE PROCEDURE TEST_PROC(query STRING)
RETURNS STRING  -- Returns the shape as a string
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'my_procedure'
AS
$$
import datetime
def my_procedure(session,query):
    # Execute the query and convert to a DataFrame
    df = session.sql(query).to_pandas()
    
    # Get the shape of the DataFrame
    
    start_time = datetime.datetime.now()
    
    if df.shape[0]>0:
        try:
            status = 'Executed successfully!'
        except Exception as e:
            status = f'Failed with error {str(e)}'
    else:
        status = f'Executed successfully -> No Failure!'
    
    return f"{start_time}, {df.shape[0]}, {status}, '{query}"
$$;

--CALL TEST_PROC('SELECT CURRENT_DATE();');



--- A Caller Function to : to audit /logs
CREATE OR REPLACE PROCEDURE DMS_OLAP_DATABASE.DMS_OLAP_NONPROD.EXECUTEPROCEDURE("PROC_NAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'procedure_handler'
EXECUTE AS CALLER
AS '
import datetime

def procedure_handler(session,proc_name):

    def call_proc(proc_name):

        start_time = datetime.datetime.now()
        try:
            session.sql(f"CALL {proc_name}()").collect()
            status = ''Executed successfully!''
        except Exception as e:
            status = f''Failed with error {str(e)}''
            
        end_time = datetime.datetime.now()
    
        # Insert execution details into the audit table
        
        session.sql(f"""
            INSERT INTO DMS_OLAP_DATABASE.META_INFO.PROCEDURE_TASK_AUDIT (PROCEDURE_NAME, START_TIME, END_TIME, DESCRIPTION)
            VALUES (''{proc_name}'', ''{start_time}'', ''{end_time}'', ''{status}'')
            """).collect()
    
        return f"{proc_name}: {status}"
        
    return call_proc(proc_name)

';


CREATE OR REPLACE TASK TASK__NAME
WAREHOUSE = COMPUTE_WH
SCHEDULE = 'USING CRON 30 7 * * * Asia/Kolkata'
AS
BEGIN
--Inline SQL
DELETE FROM DMS_OLAP_DATABASE.META_INFO.PBI_REFRESH_HISTORY
WHERE DATE(START_TIME) = CURRENT_DATE();
-- Procedure
CALL PROCEDURE_NAME()
END;

ALTER TASK TASK__NAME RESUME;

EXECUTE TASK TASK__NAME;


SELECT CURRENT_DATE();