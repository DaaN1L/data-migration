# TODO change order by
CREATE_TABLE_QUERY = """
    CREATE TABLE IF NOT EXISTS {schema}.{table}
    (
    {columns}
    )
    ENGINE = MergeTree
    ORDER BY index
"""