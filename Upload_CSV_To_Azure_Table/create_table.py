import ast
import settings
import pandas
import queries

type_priorities = [type(None), int, float, str]
string_type_name = 'nvarchar(4000)'


def get_column_type(value):
    if value == '':
        return type(None)

    try:
        literal = ast.literal_eval(value)
        if isinstance(literal, bool) or isinstance(literal, tuple):
            return str
        return type(literal)
    except SyntaxError:
        return str
    except ValueError:
        try:
            pandas.to_datetime(value, 'raise')
            return pandas.datetime
        except Exception:
            return str


def get_priority(name):
    try:
        return type_priorities.index(name)
    except ValueError:
        return -1


def get_types(reader, number_columns):
    types = []
    indices = [i for i in range(number_columns)]

    for row in reader:
        next_indices = []
        for index in indices:
            column_type = get_column_type(row[index])
            if index < len(types):
                type_index = max(
                    get_priority(column_type),
                    get_priority(types[index]))
                if type_index >= 0:
                    column_type = type_priorities[type_index]
                if type_index < len(type_priorities) - 1:
                    next_indices.append(index)
                types[index] = column_type
            else:
                types.append(column_type)
                next_indices.append(index)
        indices = next_indices

    return types


def type_to_name(t):
    if isinstance(None, t):
        return string_type_name

    if t != str:
        if t != pandas.datetime:
            return t.__name__
        return 'datetime'
    return string_type_name


def filename_to_tablename(filename):
    return filename[:filename.index('.')]


def create_table(cursor, filename, reader):

    table_name = filename_to_tablename(filename)
    if queries.has_table(cursor, table_name):
        if settings.drop_table_if_existing:
            queries.drop_table(cursor, table_name)
            print("Table already exists for " + filename + ".  Dropping table.")
        else:
            print("Table already exists for "+filename+".  Skipping table creation.")
            return

    print('Creating table for ' + filename)
    header_row = next(reader)
    if header_row is None:
        print("Invalid header in "+filename+", skipping table creation.")
        return

    types = get_types(reader, len(header_row))

    column_list = []
    for i in range(0, len(header_row)):
        column_list.append(' '.join(['\"'+header_row[i]+'\"', type_to_name(types[i])]))

    create_query = "CREATE TABLE [" + settings.schema_name + "].[" + table_name + "](" + ', '.join(column_list) + ")"

    cursor.execute(create_query)
