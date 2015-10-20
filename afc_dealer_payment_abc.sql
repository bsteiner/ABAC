/*
SELECT * FROM pcm_afc_measure WHERE subject_area = 'WF_COS_300_AFC_DEALER_PAYMENT';
*/


DECLARE
  v_workflow              pcm_afc_measure.subject_area%TYPE            := 'WF_COS_300_AFC_DEALER_PAYMENT';
  v_measure_cd            pcm_afc_measure.measure_cd%TYPE;      
  v_measure_name          pcm_afc_measure.measure_name%TYPE;
  v_measure_desc          pcm_afc_measure.measure_desc%TYPE;
  v_critical_measure_ind  pcm_afc_measure.critical_measure_ind%TYPE;
  v_source_sql            pcm_afc_measure.source_sql%TYPE;
  v_target_sql            pcm_afc_measure.target_sql%TYPE;
  v_consolidated_sql      pcm_afc_measure.consolidated_sql%TYPE;
  v_common_measure_cd     pcm_afc_measure.common_measure_cd%TYPE;

BEGIN

/*****************************************************************************/
DELETE FROM bi_afc_stg.pcm_afc_measure
 WHERE subject_area = v_workflow;
 
 /*****************************************************************************/
v_measure_cd            := 'AFC_DLR_PAYMENT_R01';
v_measure_name          := 'AFC_DLR_PAYMENT - CNT OF FIN_EVENTS';
v_measure_desc          := 'COUNT(*)';
v_critical_measure_ind  := NULL;
v_consolidated_sql      := 
'SELECT measure_cd, src_fe_id, afc_fe_id FROM (
WITH afc AS (
  SELECT DISTINCT financial_event_id
    FROM bi_afc_cdw.afc_dealer_payment
), src AS (
  SELECT fe.financial_event_id
    FROM bi_src_cosmos.src_c_financial_events fe
    JOIN bi_src_cosmos.src_c_legal_entities le
         ON fe.payor_le_id = le.legal_entity_id
  WHERE le.business_type_cd = ''ADEA''
    AND fe.financial_event_type_id = 1
    AND fe.payee_le_id = 5000
    AND fe.bi_record_status <> ''D''
    AND le.bi_record_status <> ''D''
)
SELECT '''|| v_measure_cd ||''' AS MEASURE_CD,
       COUNT(src.financial_event_id) AS src_fe_id,
       COUNT(afc.financial_event_id) AS afc_fe_id
    FROM src
    FULL OUTER JOIN afc
         ON src.financial_event_id = afc.financial_event_id
      )
      ';
          
v_common_measure_cd     := NULL;
v_source_sql            := NULL;
v_target_sql            := NULL;

INSERT INTO bi_afc_stg.pcm_afc_measure 
  (measure_cd, measure_name, measure_desc, subject_area, critical_measure_ind, 
   source_sql, target_sql, consolidated_sql, common_measure_cd)
VALUES (v_measure_cd, v_measure_name, v_measure_desc, v_workflow, 
        v_critical_measure_ind, v_source_sql, v_target_sql, v_consolidated_sql, 
        v_common_measure_cd);
 
/*****************************************************************************/
v_measure_cd            := 'AFC_DLR_PAYMENT_R02';
v_measure_name          := 'AFC_DLR_PAYMENT - CNT OF FIN_EVENTS';
v_measure_desc          := 'COUNT UNMATCHED';
v_critical_measure_ind  := NULL;
v_consolidated_sql      := 
'SELECT measure_cd, src_fe_id, afc_fe_id FROM (
WITH afc AS (
  SELECT DISTINCT financial_event_id
    FROM bi_afc_cdw.afc_dealer_payment
), src AS (
  SELECT fe.financial_event_id
    FROM bi_src_cosmos.src_c_financial_events fe
    JOIN bi_src_cosmos.src_c_legal_entities le
         ON fe.payor_le_id = le.legal_entity_id
  WHERE le.business_type_cd = ''ADEA''
    AND fe.financial_event_type_id = 1
    AND fe.payee_le_id = 5000
    AND fe.bi_record_status <> ''D''
    AND le.bi_record_status <> ''D''
)
SELECT '''|| v_measure_cd ||''' AS MEASURE_CD,
       0 AS src_fe_id,
       COUNT(src.financial_event_id) + COUNT(afc.financial_event_id) AS afc_fe_id
    FROM src
    FULL OUTER JOIN afc
         ON src.financial_event_id = afc.financial_event_id
   WHERE (src.financial_event_id IS NULL
          OR afc.financial_event_id IS NULL)
      )
      ';
          
v_common_measure_cd     := NULL;
v_source_sql            := NULL;
v_target_sql            := NULL;

INSERT INTO bi_afc_stg.pcm_afc_measure 
  (measure_cd, measure_name, measure_desc, subject_area, critical_measure_ind, 
   source_sql, target_sql, consolidated_sql, common_measure_cd)
VALUES (v_measure_cd, v_measure_name, v_measure_desc, v_workflow, 
        v_critical_measure_ind, v_source_sql, v_target_sql, v_consolidated_sql, 
        v_common_measure_cd);

/*****************************************************************************/
v_measure_cd            := 'AFC_DLR_PAYMENT_R03';
v_measure_name          := 'AFC_DLR_PAYMENT_R03 - CNT OF LATEST_EVENT_IND=Y';
v_measure_desc          := 'COUNT OF LATEST_EVENT_IND=Y';
v_critical_measure_ind  := NULL;
v_consolidated_sql      := NULL;   
v_common_measure_cd     := NULL;
v_source_sql            := 
'SELECT '''|| v_measure_cd ||''' AS MEASURE_CD,
        0 AS RECONCILIATION1
  FROM bi_src_cosmos.src_c_financial_events';
  
v_target_sql            := 
'SELECT measure_cd, tgt_latest_event_ind FROM (
With Latest as
  (select distinct financial_event_id from afc_dealer_payment where latest_event_ind = ''Y''), 
  alldp as (select distinct financial_event_id from afc_dealer_payment), 
  filtered as ( 
    select alldp.financial_event_id feid_all, Latest.financial_event_id feid_latest
    from
    alldp
    left join
    Latest
    on alldp.financial_event_id = Latest.financial_event_id
    where Latest.financial_event_id is null
    )
  select '''|| v_measure_cd ||''' AS MEASURE_CD, count(1) as tgt_latest_event_ind
  from filtered
  )';

INSERT INTO bi_afc_stg.pcm_afc_measure 
  (measure_cd, measure_name, measure_desc, subject_area, critical_measure_ind, 
   source_sql, target_sql, consolidated_sql, common_measure_cd)
VALUES (v_measure_cd, v_measure_name, v_measure_desc, v_workflow, 
        v_critical_measure_ind, v_source_sql, v_target_sql, v_consolidated_sql, 
        v_common_measure_cd);



COMMIT;
END;
/
