/*
SELECT * FROM pcm_afc_measure WHERE subject_area = 'WF_COS_300_AFC_DEALER_CHARGE';
*/


DECLARE
  v_workflow              pcm_afc_measure.subject_area%TYPE            := 'WF_COS_300_AFC_DEALER_CHARGE';
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

v_measure_cd            := 'AFC_DLR_CHRG_R04';
v_measure_name          := 'AFC_DLR_CHRG_R04 - CNT';
v_measure_desc          := 'COUNT(duplicate latst_evnt_ind=Y)';
v_critical_measure_ind  := NULL;
v_consolidated_sql      := NULL;
v_common_measure_cd     := NULL;

v_source_sql            := 
'SELECT '''|| v_measure_cd ||''' AS MEASURE_CD,
        0 AS RECONCILIATION1
  from dual';
 
v_target_sql            := 
'select '''|| v_measure_cd ||''' AS MEASURE_CD, count(*) from(
  SELECT financial_event_id, count(*)
  FROM bi_afc_cdw.afc_dealer_charge
  WHERE latest_event_ind = ''Y''
  GROUP BY financial_event_id
  having count(latest_event_ind) <> 1
)
';

INSERT INTO bi_afc_stg.pcm_afc_measure 
  (measure_cd, measure_name, measure_desc, subject_area, critical_measure_ind, 
   source_sql, target_sql, consolidated_sql, common_measure_cd)
VALUES (v_measure_cd, v_measure_name, v_measure_desc, v_workflow, 
        v_critical_measure_ind, v_source_sql, v_target_sql, v_consolidated_sql, 
        v_common_measure_cd);

/*****************************************************************************/
/*****************************************************************************/

v_measure_cd            := 'AFC_DLR_CHRG_R05';
v_measure_name          := 'AFC_DLR_CHRG_R05 - CNT';
v_measure_desc          := 'COUNT(writeoffs after chgdtm)';
v_critical_measure_ind  := NULL;
v_consolidated_sql      := NULL;
v_common_measure_cd     := NULL;

v_source_sql            := 
'SELECT '''|| v_measure_cd ||''' AS MEASURE_CD,
        0 AS RECONCILIATION1
  FROM dual';
 
v_target_sql            := 
'select '''|| v_measure_cd ||''' AS MEASURE_CD, count(*) from(
  WITH writeoffs AS (
     SELECT contract_id,
            TRUNC(min(created_dtm)) AS writeoff_date
       FROM bi_src_cosmos.src_c_financial_events
      WHERE financial_event_type_id = 4
      GROUP BY contract_id
),
base AS (
SELECT w.contract_id, w.writeoff_date, TRUNC(dc.effective_date) AS effective_date, dc.financial_event_id
  FROM bi_afc_cdw.afc_dealer_charge dc
  LEFT JOIN writeoffs w
       ON w.contract_id = dc.sales_contract_id
)

SELECT contract_id as cnt
  FROM base
 WHERE contract_id IS NOT NULL
   AND (TRUNC(effective_date) >= writeoff_date
        OR writeoff_date IS NULL)
       
)
';

INSERT INTO bi_afc_stg.pcm_afc_measure 
  (measure_cd, measure_name, measure_desc, subject_area, critical_measure_ind, 
   source_sql, target_sql, consolidated_sql, common_measure_cd)
VALUES (v_measure_cd, v_measure_name, v_measure_desc, v_workflow, 
        v_critical_measure_ind, v_source_sql, v_target_sql, v_consolidated_sql, 
        v_common_measure_cd);

/*****************************************************************************/
COMMIT;
END;
/
