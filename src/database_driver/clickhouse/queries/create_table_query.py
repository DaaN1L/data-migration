# TODO change order by
CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS {schema}.{table}
    (
    {columns}
    )
    ENGINE = ReplacingMergeTree
    ORDER BY index
    
"""