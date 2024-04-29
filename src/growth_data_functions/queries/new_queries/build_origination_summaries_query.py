from datetime import datetime, timedelta

def build_origination_summaries_query(
        project: str,
        database: str,
        table: str,
        start_date:str = "2024-01-01",
        end_date:str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
) -> str:
    """
    Query to retrive the base origination data.
    
    Args:
        project (str): The bigquery project.
        database (str): The bigquery database.
        table (str): The table where are the clients data to retrieve.
    
    Returns:
        query (str): A query to retrieve the basic origination data.
    """

    # check for empty strings
    if project == '' or database == '' or table == '':
        raise ValueError("project, database, and table must be non-empty strings.")
    
    # Change time to Manaus
    query = f"""
    
    WITH origination_data AS (
    SELECT 
        id,
        financedValue,
        originationTimestamp,
        DATE(TIMESTAMP_SUB(originationTimestamp, INTERVAL 4 HOUR)) as orig_date,
        DATE(TIMESTAMP_TRUNC(TIMESTAMP_SUB(originationTimestamp, INTERVAL 4 HOUR), MONTH)) as orig_month,
        retailerId,
        borrowerId,
        contractId,
        installmentValue,
        numberOfInstallments,
        `prd-ume-data.prd_datastore_public.origination_summaries`.canceledOn
    FROM `{project}.{database}.{table}`
    WHERE DATE(originationTimestamp) >= DATE('{start_date}')
    AND DATE(originationTimestamp) <= DATE('{end_date}')
    AND `prd-ume-data.prd_datastore_public.origination_summaries`.canceledOn is null
)

SELECT 
    id,
    financedValue,
    originationTimestamp,
    orig_date,
    orig_month,
    retailerId,
    borrowerId,
    contractId,
    installmentValue,
    numberOfInstallments,
    DATE(MIN(orig_date) OVER (PARTITION BY borrowerId)) as conv_date,
    DATE(MIN(orig_month) OVER (PARTITION BY borrowerId)) as conv_month,
    ROW_NUMBER() OVER (PARTITION BY borrowerId ORDER BY orig_date) as n_compra,
    ROW_NUMBER() OVER (PARTITION BY borrowerId ORDER BY orig_date) - 1 as n_ultima_compra,
    ROW_NUMBER() OVER (PARTITION BY borrowerId ORDER BY orig_date) - 2 as n_penultima_compra
FROM origination_data
    """

    return query


# Fazer uma get pro cliente e uma get pras stores