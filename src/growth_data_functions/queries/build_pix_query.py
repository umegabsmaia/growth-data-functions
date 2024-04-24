def build_pix_query(
        project: str,
        database: str,
        table: str
) -> str:
    """
    Query to retrive the clients pix group data.
    
    Args:
        project (str): The bigquery project.
        database (str): The bigquery database.
        table (str): The table where are the clients data to retrieve.
    
    Returns:
        query (str): A query to retrieve the clients pix group data  
    """

    # check for empty strings
    if project == '' or database == '' or table == '':
        raise ValueError("project, database, and table must be non-empty strings.")
    
    query = f"""
        WITH RankedMovimentation AS (
    SELECT 
        borrowerId, 
        start_date,     
        end_date, 
        note, 
        rede, 
        grupo, 
        n_originado,
        ROW_NUMBER() OVER (PARTITION BY borrowerId, start_date ORDER BY start_date) AS row_num
    FROM 
        `{project}.{database}.{table}`
        )
    SELECT 
        borrowerId, 
        start_date, 
        end_date, 
        rede, 
        grupo
    FROM 
        RankedMovimentation
    WHERE 
        row_num = 1;
    """

    return query