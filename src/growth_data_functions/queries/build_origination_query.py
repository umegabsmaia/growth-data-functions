from datetime import datetime, timedelta

def build_origination_query(
        project:str,
        database:str,
        table:str,
        start_date:str = "2024-01-01",
        end_date:str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d"))-> str:
    """
    Query to retrieve the origination data.
    
    Args:
        project (str): The BigQuery project.
        database (str): The BigQuery database.
        table (str): The table where the client's data is stored.
        start_date (str): The start date for data retrieval. Default is "2024-01-01".
        end_date (str): The end date for data retrieval. Default is yesterday's date.
    
    Returns:
        query (str): A query to retrieve the origination data.  
    """

    
    query = f"""
    select *, 
  CASE 
    WHEN orig_month = conv_month THEN "New"
    ELSE "Recurrent"
  END as tipo_cliente,
  
  CASE 
    WHEN retailerId = app_retailer THEN 0
    ELSE 1
  END as cross_sell,
  
  CASE 
    WHEN orig_month = conv_month AND conv_date = app_date THEN "Same Day"
    WHEN orig_month = conv_month AND conv_date <> app_date THEN "Late Conversion" 
    ELSE NULL
  END as conv_type,
  
  CASE 
    WHEN orig_date = conv_date THEN DATE_DIFF(conv_date, app_date, DAY)
    ELSE NULL
  END as conv_days_since_app,
  
  ROW_NUMBER() OVER (PARTITION BY borrowerId ORDER BY originationTimestamp) as n_compra,
  ROW_NUMBER() OVER (PARTITION BY borrowerId ORDER BY originationTimestamp) - 1 as n_ultima_compra,
  ROW_NUMBER() OVER (PARTITION BY borrowerId ORDER BY originationTimestamp) - 2 as n_penultima_compra,
  CURRENT_DATE() AS hoje
  
  from (
    select `prd-ume-data.prd_datastore_public.origination_summaries`.borrowerId,
    `prd-ume-data.prd_datastore_public.origination_summaries`.originationTimestamp,
    `prd-ume-data.prd_datastore_public.origination_summaries`.financedValue,
    `prd-ume-data.prd_datastore_public.origination_summaries`.retailerId,
    `prd-ume-data.prd_datastore_public.origination_summaries`.storeId,
    `prd-ume-data.prd_datastore_public.origination_summaries`.contractId,
    `prd-ume-data.prd_datastore_public.origination_summaries`.installmentValue,
    `prd-ume-data.prd_datastore_public.origination_summaries`.numberOfInstallments,
    `prd-ume-data.prd_datastore_public.stores`.name as storeName, `prd-ume-data.prd_datastore_public.retailers`.fantasyName as retailer_name, 
    CASE
        WHEN `prd-ume-data.prd_datastore_public.origination_summaries`.storeId in (772,812,897) THEN app_city
        ELSE `prd-ume-data.prd_datastore_public.address`.city
    END as city,
    CASE
        WHEN `prd-ume-data.prd_datastore_public.origination_summaries`.storeId in (772,812,897) THEN app_state
        ELSE `prd-ume-data.prd_datastore_public.address`.federativeUnit
    END as state,
    CASE
        WHEN `prd-ume-data.prd_datastore_public.origination_summaries`.storeId in (772,812,897) THEN app_address
        ELSE `prd-ume-data.prd_datastore_public.address`.id
    END as addressId,
    app_date,
    app_retailer,
    app_retailer_name,
    app_city,
    app_state,
    DATE(`prd-ume-data.prd_datastore_public.origination_summaries`.originationTimestamp, '-04:00') as orig_date, 
    DATE_TRUNC(DATE(`prd-ume-data.prd_datastore_public.origination_summaries`.originationTimestamp, '-04:00'), MONTH) as orig_month,
    MIN(DATE(`prd-ume-data.prd_datastore_public.origination_summaries`.originationTimestamp, '-04:00')) OVER (PARTITION BY `prd-ume-data.prd_datastore_public.origination_summaries`.borrowerId) as conv_date, 
    MIN(DATE_TRUNC(DATE(`prd-ume-data.prd_datastore_public.origination_summaries`.originationTimestamp, '-04:00'), MONTH)) OVER (PARTITION BY `prd-ume-data.prd_datastore_public.origination_summaries`.borrowerId) as conv_month,
    CASE 
        WHEN `prd-ume-data.prd_datastore_public.origination_summaries`.storeId in (812,897) THEN "PL"
        WHEN `prd-ume-data.prd_datastore_public.origination_summaries`.storeId = 772 THEN "Pix"
        WHEN `prd-ume-data.prd_datastore_public.contracts`.sourceProduct = "HIGH_RECURRENCE" THEN "Ume Leve"
        ELSE "Conventional"
    END as product
    
    from `{project}.{database}.{table}`
    left join `prd-ume-data.prd_datastore_public.stores` on `prd-ume-data.prd_datastore_public.stores`.id = `prd-ume-data.prd_datastore_public.origination_summaries`.storeId
    left join `prd-ume-data.prd_datastore_public.retailers` on `prd-ume-data.prd_datastore_public.retailers`.id = `prd-ume-data.prd_datastore_public.stores`.retailerId
    left join `prd-ume-data.prd_datastore_public.address` on `prd-ume-data.prd_datastore_public.address`.id = `prd-ume-data.prd_datastore_public.stores`.addressId
    LEFT JOIN `prd-ume-data.prd_datastore_public.contracts` ON `prd-ume-data.prd_datastore_public.contracts`.id = `prd-ume-data.prd_datastore_public.origination_summaries`.contractId
    left join (
      select distinct 
      `prd-ume-data.prd_datastore_public.applications`.borrowerId as id_cliente,
      DATE(`prd-ume-data.prd_datastore_public.applications`.createdOn, "-4:00") as app_date,
      `prd-ume-data.prd_datastore_public.stores`.retailerId as app_retailer,
      `prd-ume-data.prd_datastore_public.retailers`.fantasyName as app_retailer_name,
      `prd-ume-data.prd_datastore_public.address`.city as app_city,
      `prd-ume-data.prd_datastore_public.address`.federativeUnit as app_state,
      `prd-ume-data.prd_datastore_public.address`.id as app_address,
      ROW_NUMBER() OVER (PARTITION BY `prd-ume-data.prd_datastore_public.applications`.borrowerId ORDER BY `prd-ume-data.prd_datastore_public.applications`.createdOn asc) as app_rn
      
      from `prd-ume-data.prd_datastore_public.applications`
      left join `prd-ume-data.prd_datastore_public.stores` on `prd-ume-data.prd_datastore_public.stores`.id = `prd-ume-data.prd_datastore_public.applications`.storeId
      left join `prd-ume-data.prd_datastore_public.address` on `prd-ume-data.prd_datastore_public.stores`.addressId = `prd-ume-data.prd_datastore_public.address`.id
      left join `prd-ume-data.prd_datastore_public.retailers` on `prd-ume-data.prd_datastore_public.stores`.retailerId = `prd-ume-data.prd_datastore_public.retailers`.id
      where `prd-ume-data.prd_datastore_public.applications`.status = "APPROVED" 
    ) on id_cliente = `prd-ume-data.prd_datastore_public.origination_summaries`.borrowerId AND app_rn = 1
    WHERE `prd-ume-data.prd_datastore_public.origination_summaries`.canceledOn is null
  ) WHERE 1=1
  AND orig_date >= DATE('{start_date}')
  AND orig_date <= DATE('{end_date}')
    """

    return query