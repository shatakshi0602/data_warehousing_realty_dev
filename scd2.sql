CREATE OR REPLACE PROCEDURE OLAP_SCD2_OPPORTUNITY_CDC_LOAD1()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

let filepath STRING := '@OLAP.STAGEING.PQ_STAGE/datalake/cdc/opportunity/' || 
                           REPLACE(CURRENT_DATE()::STRING, '-', '_') ;

let qry1 STRING := 'TRUNCATE TABLE OLAP.STAGEING.OPPORTUNITY_CDC_SOURCE';
EXECUTE IMMEDIATE : qry1;

let qry2 STRING := 'TRUNCATE TABLE OLAP.STAGEING.OPPORTUNITY_TEMP_CDC';
EXECUTE IMMEDIATE : qry2;


let qry3 STRING :=
'COPY INTO OLAP.STAGEING.OPPORTUNITY_TEMP_CDC 
FROM' || filepath;
EXECUTE IMMEDIATE : qry3;

LET qry4 STRING :=
'INSERT INTO OLAP.STAGEING.OPPORTUNITY_CDC_SOURCE
        SELECT ACCOUNT_ID,CLOSE_DATE,CREATED_AT,OPEN_DATE,OPPORTUNITY_ID,OWNER_ID,STATUS_ID,UPDATED_AT,VALUE,NULL AS valid_to,CURRENT_TIMESTAMP() AS valid_from, True AS valid_flag FROM (
            SELECT *, row_number() over(partition by OPPORTUNITY_ID order by UPDATED_AT desc) AS R
            FROM 
(
    SELECT 
        $1:ACCOUNTID::INT AS ACCOUNT_ID,
        REPLACE($1:CLOSEDATE, ''"'', '''') AS CLOSE_DATE,
        REPLACE($1:CREATED_AT, ''"'', '''') AS CREATED_AT,
        REPLACE($1:OPENDATE, ''"'', '''') AS OPEN_DATE,
        $1:OPPORTUNITYID::INT AS OPPORTUNITY_ID,
        $1:OWNERID::INT AS OWNER_ID,
        $1:STATUSID::INT AS STATUS_ID,
        REPLACE($1:UPDATED_AT, ''"'', '''') AS UPDATED_AT,
        $1:VALUE::INT AS VALUE
    FROM OLAP.STAGEING.OPPORTUNITY_TEMP_CDC
)
)WHERE R=1';

EXECUTE IMMEDIATE : qry4;

RETURN qry1 || qry2 || qry3 || qry4;
END;
$$;

CALL  OLAP_SCD2_OPPORTUNITY_CDC_LOAD1();





CREATE OR REPLACE PROCEDURE OLAP_SCD2_OPPORTUNITY_CDC_LOAD2()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

let qry1 STRING:= 
'MERGE INTO OLAP.REALTY_DEV.OPPORTUNITY AS f
USING (
SELECT p.opportunity_id as mergekey,p.* from OLAP.STAGEING.OPPORTUNITY_CDC_SOURCE as p

UNION ALL

SELECT NULL as mergekey, s.* from OLAP.STAGEING.OPPORTUNITY_CDC_SOURCE as s

JOIN OLAP.REALTY_DEV.OPPORTUNITY d
ON s.opportunity_id = d.opportunity_id
WHERE d.valid_flag = TRUE
AND CONCAT(s.account_id ,''|'', s.close_date ,''|'', s.created_at ,''|'', s.open_date ,''|'', s.opportunity_id ,''|'', s.owner_id ,''|'', s.status_id ,''|'', s.updated_at ,''|'', s.value)
<> CONCAT(d.account_id ,''|'', d.close_date ,''|'', d.created_at ,''|'', d.open_date ,''|'', d.opportunity_id ,''|'', d.owner_id ,''|'', d.status_id ,''|'', d.updated_at ,''|'', d.value)
)sp

ON f.opportunity_id = sp.mergekey

WHEN MATCHED
AND f.valid_flag = TRUE
AND CONCAT(sp.account_id ,''|'', sp.close_date ,''|'', sp.created_at ,''|'', sp.open_date ,''|'', sp.opportunity_id ,''|'', sp.owner_id ,''|'', sp.status_id ,''|'', sp.updated_at ,''|'', sp.value)
<> CONCAT(f.account_id ,''|'', f.close_date ,''|'', f.created_at ,''|'', f.open_date ,''|'', f.opportunity_id ,''|'', f.owner_id ,''|'', f.status_id ,''|'', f.updated_at ,''|'', f.value)

THEN UPDATE SET f.valid_flag = FALSE , f.valid_to =  CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT(account_id, close_date, created_at, open_date, opportunity_id, owner_id, status_id, updated_at, value,  valid_from, valid_to,valid_flag)
VALUES(sp.account_id, sp.close_date, sp.created_at, sp.open_date, sp.opportunity_id, sp.owner_id, sp.status_id, sp.updated_at, sp.value, CURRENT_TIMESTAMP(), NULL, TRUE)';

EXECUTE IMMEDIATE qry1;

    RETURN qry1;
END;
$$;

call OLAP_SCD2_OPPORTUNITY_CDC_LOAD2();

select * from OLAP.REALTY_DEV.OPPORTUNITY;
---------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE OLAP_SCD2_PAYMENTS_CDC_LOAD1()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

let filepath STRING := '@OLAP.STAGEING.PQ_STAGE/datalake/cdc/payments/' || 
                           REPLACE(CURRENT_DATE()::STRING, '-', '_') ;

let qry1 STRING := 'TRUNCATE TABLE OLAP.STAGEING.PAYMENTS_CDC_SOURCE';
 EXECUTE IMMEDIATE : qry1;

 let qry2 STRING := 'TRUNCATE TABLE OLAP.STAGEING.PAYMENTS_TEMP_CDC';
 EXECUTE IMMEDIATE : qry2;



let qry3 STRING :=
'COPY INTO OLAP.STAGEING.PAYMENTS_TEMP_CDC 
FROM' || filepath;
EXECUTE IMMEDIATE : qry3;



RETURN  qry1||qry2||qry3 ;
END;
$$;

CALL  OLAP_SCD2_PAYMENTS_CDC_LOAD1();


CREATE OR REPLACE PROCEDURE OLAP_PAYMENTS_CDC_LOAD_2()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

LET qry1 STRING :=
'INSERT INTO OLAP.STAGEING.PAYMENTS_CDC_SOURCE  (
SELECT PAYMENTID,RECEIPTDATE,CLEARANCEDATE,DEBITDATE,CREATED_AT,OPPORTUNITYID,MODEID,STATUSID,UPDATED_AT,AMOUNT,TDSDEDUCTED,NULL AS VALID_TO,CURRENT_TIMESTAMP() AS VALID_FROM,True AS VALID_FLAG FROM (
            SELECT *, row_number() over(partition by PAYMENTID order by UPDATED_AT desc) AS R
            FROM 
(
    SELECT 
        $1:PAYMENTID::INT AS PAYMENTID,
        REPLACE($1:RECEIPTDATE, ''"'', '''') AS RECEIPTDATE,
        REPLACE($1:CLEARANCEDATE, ''"'', '''') AS CLEARANCEDATE,
        REPLACE($1:DEBITDATE, ''"'', '''') AS DEBITDATE,
        REPLACE($1:CREATED_AT, ''"'', '''') AS CREATED_AT,
        $1:OPPORTUNITYID::INT AS OPPORTUNITYID,
        $1:MODEID::INT AS MODEID,
        $1:STATUSID::INT AS STATUSID,
        REPLACE($1:UPDATED_AT, ''"'', '''') AS UPDATED_AT,
        $1:AMOUNT::INT AS AMOUNT,
        $1:TDSDEDUCTED::INT AS TDSDEDUCTED
       
    FROM OLAP.STAGEING.PAYMENTS_TEMP_CDC
)
)WHERE R=1)';

EXECUTE IMMEDIATE  qry1;

let qry2 STRING:= 
'MERGE INTO OLAP.REALTY_DEV.PAYMENTS AS f
USING (
SELECT p.paymentid as mergekey,p.* from OLAP.STAGEING.PAYMENTS_CDC_SOURCE as p

UNION ALL

SELECT NULL as mergekey, s.* from OLAP.STAGEING.PAYMENTS_CDC_SOURCE as s

JOIN OLAP.REALTY_DEV.PAYMENTS d
ON s.paymentid = d.paymentid
WHERE d.valid_flag = TRUE
AND CONCAT(s.PAYMENTID ,''|'', s.RECEIPTDATE ,''|'', s.CLEARANCEDATE ,''|'', s.DEBITDATE ,''|'', s.CREATED_AT ,''|'', s.OPPORTUNITYID ,''|'', s.MODEID ,''|'', s.STATUSID ,''|'', s.UPDATED_AT ,''|'', s.AMOUNT ,''|'', s.TDSDEDUCTED)
<> CONCAT(d.PAYMENTID ,''|'', d.RECEIPTDATE ,''|'', d.CLEARANCEDATE ,''|'', d.DEBITDATE ,''|'', d.CREATED_AT ,''|'', d.OPPORTUNITYID ,''|'', d.MODEID ,''|'', d.STATUSID ,''|'', d.UPDATED_AT ,''|'', d.AMOUNT ,''|'', d.TDSDEDUCTED)
)sp

ON f.paymentid = sp.mergekey

WHEN MATCHED
AND f.valid_flag = TRUE
AND CONCAT(sp.PAYMENTID ,''|'', sp.RECEIPTDATE ,''|'', sp.CLEARANCEDATE ,''|'', sp.DEBITDATE ,''|'', sp.CREATED_AT ,''|'', sp.OPPORTUNITYID ,''|'', sp.MODEID ,''|'', sp.STATUSID ,''|'', sp.UPDATED_AT ,''|'', sp.AMOUNT ,''|'', sp.TDSDEDUCTED)
<> CONCAT(f.PAYMENTID ,''|'', f.RECEIPTDATE ,''|'', f.CLEARANCEDATE ,''|'', f.DEBITDATE ,''|'', f.CREATED_AT ,''|'', f.OPPORTUNITYID ,''|'', f.MODEID ,''|'', f.STATUSID ,''|'', f.UPDATED_AT ,''|'', f.AMOUNT ,''|'', f.TDSDEDUCTED)

THEN UPDATE SET f.valid_flag = FALSE , f.valid_to = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT(PAYMENTID,RECEIPTDATE,CLEARANCEDATE,DEBITDATE,CREATED_AT,OPPORTUNITYID,MODEID,STATUSID,UPDATED_AT,AMOUNT,TDSDEDUCTED, VALID_FROM,VALID_TO, VALID_FLAG)
VALUES(SP.PAYMENTID,SP.RECEIPTDATE,SP.CLEARANCEDATE,SP.DEBITDATE,SP.CREATED_AT,SP.OPPORTUNITYID,SP.MODEID,SP.STATUSID,SP.UPDATED_AT,SP.AMOUNT,SP.TDSDEDUCTED, CURRENT_TIMESTAMP(), NULL, TRUE)';

EXECUTE IMMEDIATE qry2;

    RETURN qry1||qry2;
END;
$$;

call OLAP_PAYMENTS_CDC_LOAD_2();
select * from OLAP.REALTY_DEV.PAYMENTS;
-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE OLAP_SCD2_OPPORTUNITY_STATUS_CDC_LOAD1()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

let filepath STRING := '@OLAP.STAGEING.PQ_STAGE/datalake/cdc/opportunity_status/' || 
                           REPLACE(CURRENT_DATE()::STRING, '-', '_') ;

let qry1 STRING := 'truncate TABLE OLAP.STAGEING.OPPORTUNITY_STATUS_CDC_SOURCE';
 EXECUTE IMMEDIATE : qry1;

 let qry2 STRING := 'truncate TABLE OLAP.STAGEING.OPPORTUNITY_STATUS_TEMP_CDC';
 EXECUTE IMMEDIATE : qry2;


let qry3 STRING :=
'COPY INTO OLAP.STAGEING.OPPORTUNITY_STATUS_TEMP_CDC 
FROM' || filepath;
EXECUTE IMMEDIATE : qry3;



RETURN  qry1||qry2||qry3 ;
END;
$$;

CALL  OLAP_SCD2_OPPORTUNITY_STATUS_CDC_LOAD1();


CREATE OR REPLACE PROCEDURE OLAP_SCD2_OPPORTUNITY_STATUS_CDC_LOAD2()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

LET qry1 STRING :=
'insert into  OLAP.STAGEING.OPPORTUNITY_STATUS_CDC_SOURCE  (
SELECT STATUSID,DESCRIPTION,CREATED_AT,UPDATED_AT, NULL AS VALID_TO,CURRENT_TIMESTAMP() AS VALID_FROM,True AS VALID_FLAG FROM (
            SELECT *, row_number() over(partition by STATUSID order by UPDATED_AT desc) AS R
            FROM 
(
    SELECT 
        $1:STATUSID::INT AS STATUSID,
        $1:DESCRIPTION::STRING AS DESCRIPTION,
        REPLACE($1:CREATED_AT, ''"'', '''') AS CREATED_AT,
        REPLACE($1:UPDATED_AT, ''"'', '''') AS UPDATED_AT
       
    FROM OLAP.STAGEING.OPPORTUNITY_STATUS_TEMP_CDC
)
)WHERE R=1)';

EXECUTE IMMEDIATE : qry1;

let qry2 STRING:= 
'MERGE INTO OLAP.REALTY_DEV.OPPORTUNITY_STATUS AS f
USING (
SELECT p.statusid as mergekey,p.* from OLAP.STAGEING.OPPORTUNITY_STATUS_CDC_SOURCE as p

UNION ALL

SELECT NULL as mergekey, s.* from OLAP.STAGEING.OPPORTUNITY_STATUS_CDC_SOURCE as s

JOIN OLAP.REALTY_DEV.OPPORTUNITY_STATUS d
ON s.statusid = d.statusid
WHERE d.valid_flag = TRUE
AND CONCAT(s.STATUSID ,''|'', s.DESCRIPTION ,''|'', s.CREATED_AT ,''|'', s.UPDATED_AT)
<> CONCAT(d.STATUSID ,''|'', d.DESCRIPTION ,''|'', d.CREATED_AT ,''|'', d.UPDATED_AT)
)sp

ON f.statusid = sp.mergekey

WHEN MATCHED
AND f.valid_flag = TRUE
AND CONCAT(sp.STATUSID ,''|'', sp.DESCRIPTION ,''|'', sp.CREATED_AT ,''|'', sp.UPDATED_AT)
<> CONCAT(f.STATUSID ,''|'', f.DESCRIPTION ,''|'', f.CREATED_AT ,''|'', f.UPDATED_AT)

THEN UPDATE SET f.valid_flag = FALSE , f.valid_to = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT(STATUSID,DESCRIPTION,CREATED_AT,UPDATED_AT,VALID_FROM,VALID_TO,VALID_FLAG )
VALUES(SP.STATUSID,SP.DESCRIPTION,SP.CREATED_AT,SP.UPDATED_AT, CURRENT_TIMESTAMP(), NULL, TRUE)';

EXECUTE IMMEDIATE qry2;

    RETURN qry1||qry2;
END;
$$;

call OLAP_SCD2_OPPORTUNITY_STATUS_CDC_LOAD2();




-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE OLAP_SCD2_PAYMENT_MODE_CDC_LOAD1()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

let filepath STRING := '@OLAP.STAGEING.PQ_STAGE/datalake/cdc/payment_mode/' || 
                           REPLACE(CURRENT_DATE()::STRING, '-', '_') ;

let qry1 STRING := 'TRUNCATE TABLE OLAP.STAGEING.PAYMENT_MODE_CDC_SOURCE';
 EXECUTE IMMEDIATE : qry1;

 let qry2 STRING := 'TRUNCATE TABLE OLAP.STAGEING.PAYMENT_MODE_TEMP_CDC';
 EXECUTE IMMEDIATE : qry2;



let qry3 STRING :=
'COPY INTO OLAP.STAGEING.PAYMENT_MODE_TEMP_CDC 
FROM' || filepath;
EXECUTE IMMEDIATE : qry3;



RETURN  qry1||qry2||qry3 ;
END;
$$;

CALL  OLAP_SCD2_PAYMENT_MODE_CDC_LOAD1();


CREATE OR REPLACE PROCEDURE OLAP_SCD2_PAYMENT_MODE_CDC_LOAD2()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

LET qry1 STRING :=
'INSERT INTO OLAP.STAGEING.PAYMENT_MODE_CDC_SOURCE  (
SELECT MODEID,DESCRIPTION,CREATED_AT,UPDATED_AT, NULL AS VALID_TO,CURRENT_TIMESTAMP() AS VALID_FROM,True AS VALID_FLAG FROM (
            SELECT *, row_number() over(partition by MODEID order by UPDATED_AT desc) AS R
            FROM 
(
    SELECT 
        $1:MODEID::INT AS MODEID,
        $1:DESCRIPTION::STRING AS DESCRIPTION,
        REPLACE($1:CREATED_AT, ''"'', '''') AS CREATED_AT,
        REPLACE($1:UPDATED_AT, ''"'', '''') AS UPDATED_AT
       
    FROM OLAP.STAGEING.PAYMENT_MODE_TEMP_CDC
)
)WHERE R=1)';

EXECUTE IMMEDIATE : qry1;

let qry2 STRING:= 
'MERGE INTO OLAP.REALTY_DEV.PAYMENT_MODE AS f
USING (
SELECT p.modeid as mergekey,p.* from OLAP.STAGEING.PAYMENT_MODE_CDC_SOURCE as p

UNION ALL

SELECT NULL as mergekey, s.* from OLAP.STAGEING.PAYMENT_MODE_CDC_SOURCE as s

JOIN OLAP.REALTY_DEV.PAYMENT_MODE d
ON s.modeid = d.modeid
WHERE d.valid_flag = TRUE
AND CONCAT(s.MODEID ,''|'', s.DESCRIPTION ,''|'', s.CREATED_AT ,''|'', s.UPDATED_AT)
<> CONCAT(d.MODEID ,''|'', d.DESCRIPTION ,''|'', d.CREATED_AT ,''|'', d.UPDATED_AT)
)sp

ON f.modeid = sp.mergekey

WHEN MATCHED
AND f.valid_flag = TRUE
AND CONCAT(sp.MODEID ,''|'', sp.DESCRIPTION ,''|'', sp.CREATED_AT ,''|'', sp.UPDATED_AT)
<> CONCAT(f.MODEID ,''|'', f.DESCRIPTION ,''|'', f.CREATED_AT ,''|'', f.UPDATED_AT)

THEN UPDATE SET f.valid_flag = FALSE ,f.valid_to = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT(MODEID,DESCRIPTION,CREATED_AT,UPDATED_AT,VALID_FROM,VALID_TO,VALID_FLAG )
VALUES(SP.MODEID,SP.DESCRIPTION,SP.CREATED_AT,SP.UPDATED_AT, CURRENT_TIMESTAMP(), NULL, TRUE)';

EXECUTE IMMEDIATE qry2;

    RETURN qry1||qry2;
END;
$$;

call OLAP_SCD2_PAYMENT_MODE_CDC_LOAD2();




-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE OLAP_SCD2_PAYMENT_STATUS_CDC_LOAD1()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

let filepath STRING := '@OLAP.STAGEING.PQ_STAGE/datalake/cdc/payment_status/' || 
                           REPLACE(CURRENT_DATE()::STRING, '-', '_') ;

let qry1 STRING := 'TRUNCATE TABLE OLAP.STAGEING.PAYMENT_STATUS_CDC_SOURCE';
 EXECUTE IMMEDIATE : qry1;

 let qry2 STRING := 'TRUNCATE TABLE OLAP.STAGEING.PAYMENT_STATUS_TEMP_CDC';
 EXECUTE IMMEDIATE : qry2;



let qry3 STRING :=
'COPY INTO OLAP.STAGEING.PAYMENT_STATUS_TEMP_CDC 
FROM' || filepath;
EXECUTE IMMEDIATE : qry3;



RETURN  qry1||qry2||qry3 ;
END;
$$;

CALL  OLAP_SCD2_PAYMENT_STATUS_CDC_LOAD1();


CREATE OR REPLACE PROCEDURE OLAP_SCD2_PAYMENT_STATUS_CDC_LOAD2()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

LET qry1 STRING :=
'insert into  OLAP.STAGEING.PAYMENT_STATUS_CDC_SOURCE  (
SELECT STATUSID,DESCRIPTION,CREATED_AT,UPDATED_AT, NULL AS VALID_TO,CURRENT_TIMESTAMP() AS VALID_FROM,True AS VALID_FLAG FROM (
            SELECT *, row_number() over(partition by STATUSID order by UPDATED_AT desc) AS R
            FROM 
(
    SELECT 
        $1:STATUSID::INT AS STATUSID,
        $1:DESCRIPTION::STRING AS DESCRIPTION,
        REPLACE($1:CREATED_AT, ''"'', '''') AS CREATED_AT,
        REPLACE($1:UPDATED_AT, ''"'', '''') AS UPDATED_AT
       
    FROM OLAP.STAGEING.PAYMENT_STATUS_TEMP_CDC
)
)WHERE R=1)';

EXECUTE IMMEDIATE : qry1;

let qry2 STRING:= 
'MERGE INTO OLAP.REALTY_DEV.PAYMENT_STATUS AS f
USING (
SELECT p.statusid as mergekey,p.* from OLAP.STAGEING.PAYMENT_STATUS_CDC_SOURCE as p

UNION ALL

SELECT NULL as mergekey, s.* from OLAP.STAGEING.PAYMENT_STATUS_CDC_SOURCE as s

JOIN OLAP.REALTY_DEV.PAYMENT_STATUS d
ON s.statusid = d.statusid
WHERE d.valid_flag = TRUE
AND CONCAT(s.STATUSID ,''|'', s.DESCRIPTION ,''|'', s.CREATED_AT ,''|'', s.UPDATED_AT)
<> CONCAT(d.STATUSID ,''|'', d.DESCRIPTION ,''|'', d.CREATED_AT ,''|'', d.UPDATED_AT)
)sp

ON f.statusid = sp.mergekey

WHEN MATCHED
AND f.valid_flag = TRUE
AND CONCAT(sp.STATUSID ,''|'', sp.DESCRIPTION ,''|'', sp.CREATED_AT ,''|'', sp.UPDATED_AT)
<> CONCAT(f.STATUSID ,''|'', f.DESCRIPTION ,''|'', f.CREATED_AT ,''|'', f.UPDATED_AT)

THEN UPDATE SET f.valid_flag = FALSE , f.valid_to = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT(STATUSID,DESCRIPTION,CREATED_AT,UPDATED_AT,VALID_FROM,VALID_TO,VALID_FLAG )
VALUES(SP.STATUSID,SP.DESCRIPTION,SP.CREATED_AT,SP.UPDATED_AT, CURRENT_TIMESTAMP(), NULL, TRUE)';

EXECUTE IMMEDIATE qry2;

    RETURN qry1||qry2;
END;
$$;

call OLAP_SCD2_PAYMENT_STATUS_CDC_LOAD2();



-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE OLAP_SCD2_ACCOUNTS_CDC_LOAD1()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

let filepath STRING := '@OLAP.STAGEING.PQ_STAGE/datalake/cdc/accounts/' || 
                           REPLACE(CURRENT_DATE()::STRING, '-', '_') ;

let qry1 STRING := 'TRUNCATE TABLE OLAP.STAGEING.ACCOUNTS_CDC_SOURCE';
 EXECUTE IMMEDIATE : qry1;

 let qry2 STRING := 'TRUNCATE TABLE OLAP.STAGEING.ACCOUNTS_TEMP_CDC';
 EXECUTE IMMEDIATE : qry2;

 let qry3 STRING :=
'COPY INTO OLAP.STAGEING.ACCOUNTS_TEMP_CDC 
FROM' || filepath;
EXECUTE IMMEDIATE : qry3;

RETURN  qry1||qry2||qry3 ;

END;
$$;

CALL  OLAP_SCD2_ACCOUNTS_CDC_LOAD1();


CREATE OR REPLACE PROCEDURE OLAP_SCD2_ACCOUNTS_CDC_LOAD2()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

LET qry1 STRING :=
'INSERT INTO OLAP.STAGEING.ACCOUNTS_CDC_SOURCE (
SELECT ACCOUNTID,ACCOUNTNAME,TYPE,OWNERID,AADHAARNUMBER,BANKDETAILS,CONTACTNUMBER,ADDRESS,ACTIVESTATUS,CREATED_AT,UPDATED_AT,
NULL AS VALID_TO,CURRENT_TIMESTAMP() AS VALID_FROM,True AS VALID_FLAG FROM (
            SELECT *, row_number() over(partition by ACCOUNTID order by UPDATED_AT desc) AS R
            FROM 
(
    SELECT 
        $1:ACCOUNTID::INT AS ACCOUNTID,
        $1:ACCOUNTNAME::STRING AS ACCOUNTNAME,
        $1:TYPE::STRING AS TYPE,
        $1:OWNERID::INT AS OWNERID,
        $1:AADHAARNUMBER::STRING AS AADHAARNUMBER,
        $1:BANKDETAILS::STRING AS BANKDETAILS,
        $1:CONTACTNUMBER::INT AS CONTACTNUMBER,
        $1:ADDRESS::STRING AS ADDRESS,
        $1:ACTIVESTATUS::STRING AS ACTIVESTATUS,
        REPLACE($1:CREATED_AT, ''"'', '''') AS CREATED_AT,
        REPLACE($1:UPDATED_AT, ''"'', '''') AS UPDATED_AT
       
    FROM OLAP.STAGEING.ACCOUNTS_TEMP_CDC
)
)WHERE R=1)';

EXECUTE IMMEDIATE : qry1;

let qry2 STRING:= 
'MERGE INTO OLAP.REALTY_DEV.ACCOUNTS AS f
USING (
SELECT p.accountid as mergekey,p.* from OLAP.STAGEING.ACCOUNTS_CDC_SOURCE as p

UNION ALL

SELECT NULL as mergekey, s.* from OLAP.STAGEING.ACCOUNTS_CDC_SOURCE as s

JOIN OLAP.REALTY_DEV.ACCOUNTS d
ON s.accountid = d.accountid
WHERE d.valid_flag = TRUE
AND CONCAT(s.ACCOUNTID ,''|'', s.ACCOUNTNAME ,''|'', s.TYPE ,''|'', s.OWNERID ,''|'', s.AADHAARNUMBER ,''|'', s.BANKDETAILS ,''|'', s.CONTACTNUMBER ,''|'', s.ADDRESS ,''|'', s.ACTIVESTATUS ,''|'', s.CREATED_AT ,''|'', s.UPDATED_AT)
<> CONCAT(d.ACCOUNTID ,''|'', d.ACCOUNTNAME ,''|'', d.TYPE ,''|'', d.OWNERID ,''|'', d.AADHAARNUMBER ,''|'', d.BANKDETAILS ,''|'', d.CONTACTNUMBER ,''|'', d.ADDRESS ,''|'', d.ACTIVESTATUS ,''|'', d.CREATED_AT ,''|'', d.UPDATED_AT)
)sp

ON f.accountid = sp.mergekey

WHEN MATCHED
AND f.valid_flag = TRUE
AND CONCAT(sp.ACCOUNTID ,''|'', sp.ACCOUNTNAME ,''|'', sp.TYPE ,''|'', sp.OWNERID ,''|'', sp.AADHAARNUMBER ,''|'', sp.BANKDETAILS ,''|'', sp.CONTACTNUMBER ,''|'', sp.ADDRESS ,''|'', sp.ACTIVESTATUS ,''|'', sp.CREATED_AT ,''|'', sp.UPDATED_AT)
<> CONCAT(f.ACCOUNTID ,''|'', f.ACCOUNTNAME ,''|'', f.TYPE ,''|'', f.OWNERID ,''|'', f.AADHAARNUMBER ,''|'', f.BANKDETAILS ,''|'', f.CONTACTNUMBER ,''|'', f.ADDRESS ,''|'', f.ACTIVESTATUS ,''|'', f.CREATED_AT ,''|'', f.UPDATED_AT)

THEN UPDATE SET f.valid_flag = FALSE , f.valid_to = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
INSERT(ACCOUNTID,ACCOUNTNAME,TYPE,OWNERID,AADHAARNUMBER,BANKDETAILS,CONTACTNUMBER,ADDRESS,ACTIVESTATUS,CREATED_AT,UPDATED_AT,VALID_FROM,VALID_TO,VALID_FLAG )
VALUES(SP.ACCOUNTID,SP.ACCOUNTNAME,SP.TYPE,SP.OWNERID,SP.AADHAARNUMBER,SP.BANKDETAILS,SP.CONTACTNUMBER,SP.ADDRESS,SP.ACTIVESTATUS,SP.CREATED_AT,SP.UPDATED_AT, CURRENT_TIMESTAMP(), NULL, TRUE)';

EXECUTE IMMEDIATE qry2;

    RETURN qry1||qry2;
END;
$$;

call OLAP_SCD2_ACCOUNTS_CDC_LOAD2();



-----------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE OLAP_SCD2_OWNERS_CDC_LOAD1()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

let filepath STRING := '@OLAP.STAGEING.PQ_STAGE/datalake/cdc/owners/' || 
                           REPLACE(CURRENT_DATE()::STRING, '-', '_') ;

let qry1 STRING := 'truncate TABLE OLAP.STAGEING.OWNERS_CDC_SOURCE';
 EXECUTE IMMEDIATE : qry1;

 let qry2 STRING := 'truncate TABLE OLAP.STAGEING.OWNERS_TEMP_CDC';
 EXECUTE IMMEDIATE : qry2;

-- EXECUTE IMMEDIATE 'CREATE TABLE OLAP.STAGEING.OWNERS_TEMP_CDC (OWNERS_DATA VARIANT)';

let qry3 STRING :=
'COPY INTO OLAP.STAGEING.OWNERS_TEMP_CDC 
FROM' || filepath;
EXECUTE IMMEDIATE : qry3;



RETURN  qry1||qry2||qry3 ;
--RETURN qry3;
END;
$$;

CALL  OLAP_SCD2_OWNERS_CDC_LOAD1();


CREATE OR REPLACE PROCEDURE OLAP_SCD2_OWNERS_CDC_LOAD2()
RETURNS STRING 
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
BEGIN

LET qry1 STRING :=
'insert into  OLAP.STAGEING.OWNERS_CDC_SOURCE  (
SELECT OWNERID,EMPCODE,NAME,DESIGNATION,REPORTINGMANAGER,BUSINESSUNIT,SALESCHANNEL,AREA,CITY,REGION,STATUS,CREATED_AT,UPDATED_AT,
NULL AS VALID_TO,CURRENT_TIMESTAMP() AS VALID_FROM,True AS VALID_FLAG FROM (
            SELECT *, row_number() over(partition by OWNERID order by UPDATED_AT desc) AS R
            FROM 
(
    SELECT 
        $1:OWNERID::INT AS OWNERID,
        $1:EMPCODE::STRING AS EMPCODE,
        $1:NAME::STRING AS NAME,
        $1:DESIGNATION::STRING AS DESIGNATION,
        $1:REPORTINGMANAGER::STRING AS REPORTINGMANAGER,
        $1:BUSINESSUNIT::STRING AS BUSINESSUNIT,
        $1:SALESCHANNEL::STRING AS SALESCHANNEL,
        $1:AREA::STRING AS AREA,
        $1:CITY::STRING AS CITY,
        $1:REGION::STRING AS REGION,
        $1:STATUS::STRING AS STATUS,
        REPLACE($1:CREATED_AT, ''"'', '''') AS CREATED_AT,
        REPLACE($1:UPDATED_AT, ''"'', '''') AS UPDATED_AT
       
    FROM OLAP.STAGEING.OWNERS_TEMP_CDC
)
)WHERE R=1)';

EXECUTE IMMEDIATE : qry1;

let qry2 STRING:= 
'MERGE INTO OLAP.REALTY_DEV.OWNERS AS f
USING (
SELECT p.ownerid as mergekey,p.* from OLAP.STAGEING.OWNERS_CDC_SOURCE as p

UNION ALL

SELECT NULL as mergekey, s.* from OLAP.STAGEING.OWNERS_CDC_SOURCE as s

JOIN OLAP.REALTY_DEV.OWNERS d
ON s.ownerid = d.ownerid
WHERE d.valid_flag = TRUE
AND CONCAT(s.OWNERID ,''|'', s.EMPCODE ,''|'', s.NAME ,''|'', s.DESIGNATION ,''|'', s.REPORTINGMANAGER ,''|'', s.BUSINESSUNIT ,''|'', s.SALESCHANNEL ,''|'', s.AREA ,''|'', s.CITY ,''|'', s.REGION ,''|'', s.STATUS ,''|'', s.CREATED_AT ,''|'', s.UPDATED_AT)
<> CONCAT(d.OWNERID ,''|'', d.EMPCODE ,''|'', d.NAME ,''|'', d.DESIGNATION ,''|'', d.REPORTINGMANAGER ,''|'', d.BUSINESSUNIT ,''|'', d.SALESCHANNEL ,''|'', d.AREA ,''|'', d.CITY ,''|'', d.REGION ,''|'', d.STATUS ,''|'', d.CREATED_AT ,''|'', d.UPDATED_AT)
)sp

ON f.ownerid = sp.mergekey

WHEN MATCHED
AND f.valid_flag = TRUE
AND CONCAT(sp.OWNERID ,''|'', sp.EMPCODE ,''|'', sp.NAME ,''|'', sp.DESIGNATION ,''|'', sp.REPORTINGMANAGER ,''|'', sp.BUSINESSUNIT ,''|'', sp.SALESCHANNEL ,''|'', sp.AREA ,''|'', sp.CITY ,''|'', sp.REGION ,''|'', sp.STATUS ,''|'', sp.CREATED_AT ,''|'', sp.UPDATED_AT)
<> CONCAT(f.OWNERID ,''|'', f.EMPCODE ,''|'', f.NAME ,''|'', f.DESIGNATION ,''|'', f.REPORTINGMANAGER ,''|'', f.BUSINESSUNIT ,''|'', f.SALESCHANNEL ,''|'', f.AREA ,''|'', f.CITY ,''|'', f.REGION ,''|'', f.STATUS ,''|'', f.CREATED_AT ,''|'', f.UPDATED_AT)

THEN UPDATE SET f.valid_flag = FALSE , f.valid_to = current_timestamp()

WHEN NOT MATCHED THEN
INSERT(OWNERID,EMPCODE,NAME,DESIGNATION,REPORTINGMANAGER,BUSINESSUNIT,SALESCHANNEL,AREA,CITY,REGION,STATUS,CREATED_AT,UPDATED_AT,VALID_FROM,VALID_TO,VALID_FLAG )
VALUES(SP.OWNERID,SP.EMPCODE,SP.NAME,SP.DESIGNATION,SP.REPORTINGMANAGER,SP.BUSINESSUNIT,SP.SALESCHANNEL,SP.AREA,SP.CITY,SP.REGION,SP.STATUS,SP.CREATED_AT,SP.UPDATED_AT, CURRENT_TIMESTAMP(), NULL, TRUE)';

EXECUTE IMMEDIATE qry2;

    RETURN qry1||qry2;
END;
$$;

call OLAP_SCD2_OWNERS_CDC_LOAD2();
select * from olap.realty_dev.owners;
select * from oltp.userinfo.owners;