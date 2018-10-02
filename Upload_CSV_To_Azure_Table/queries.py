import settings


def drop_table(cursor, table):
    cursor.execute("DROP TABLE ["+settings.schema_name+"].["+table+']')


def has_table(cursor, table_name):
    try:
        cursor.execute("SELECT TOP 1 * FROM [" + settings.schema_name + "].[" + table_name + ']')
        return True
    except Exception:
        return False