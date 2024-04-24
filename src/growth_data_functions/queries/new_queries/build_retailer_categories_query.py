def build_retailer_categories_query(
        project: str,
        database: str,
        table: str
) -> str:
    """
    Query to retrive the retailer categories query.
    
    Args:
        project (str): The bigquery project.
        database (str): The bigquery database.
        table (str): The table where are the clients data to retrieve.
    
    Returns:
        query (str): A query to retrieve the retailer categories data.  
    """

    # check for empty strings
    if project == '' or database == '' or table == '':
        raise ValueError("project, database, and table must be non-empty strings.")
    
    query = f"""
    
    SELECT 
        retailersId, 
        id as categoriesId, 
        name, 
        createdOn
    FROM (
        SELECT
            A.retailersId, 
            B.id, 
            B.name, 
            B.createdOn,
            ROW_NUMBER() OVER(PARTITION BY A.retailersId ORDER BY B.createdOn) AS row_num
        FROM
            `{project}.{database}.{table}` AS A
        LEFT JOIN 
            `prd-ume-data.prd_datastore_public.retailer_categories` AS B ON A.retailerCategoriesId = B.id
    
    ) AS rc
    WHERE row_num = 1
    
    """

    return query