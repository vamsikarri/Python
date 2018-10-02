import sys
import csv
import json
import os
import re
import shutil
from collections import OrderedDict


class HeaderTree:
    def __init__(self, name):
        self.children = []
        self.name = name
        self.names = set()
        self.sub_trees = {}

    def insert_leaf(self, index, name):
        """
        Insert a new child with the given (name, value) pair at the given index, if
        a child with the given name is not already present.
        :param index:
        :param name:
        :param value:
        :return:
        """
        if name not in self.names:
            self.children.insert(index, name)
            self.names.add(name)

    def insert_tree(self, index, name):
        """
        Insert and return a new HeaderTree at the given index with the given name.
        If a HeaderTree child of that name is already stored in this, return that instead.
        Overwrites any leaf children of this with the same name.
        :param index:
        :param name:
        :return:
        """
        if name not in self.sub_trees:
            if name not in self.names:
                self.children.insert(index, name)
                self.names.add(name)
            self.sub_trees[name] = HeaderTree(name)
        return self.sub_trees[name]

    def flatten(self):
        """
        Flatten this tree, replacing each HeaderTree child of this with a flattened version of itself.
        :return: An array of names from all the children in this, in the order added.
        """
        header = []
        for child in self.children:
            if child in self.sub_trees and len(self.sub_trees[child].children) > 0:
                header += self.sub_trees[child].flatten()
            else:
                header.append(child)
        return header


class MutableInt:
    def __init__(self):
        self.value = 0

    def __add__(self, other):
        return self.value + other

    def __iadd__(self, other):
        self.value += other
        return self

    def __sub__(self, other):
        return self.value - other

    def __isub__(self, other):
        self.value -= other
        return self

    def __lt__(self, other):
        return self.value < other

    def __gt__(self, other):
        return self.value > other

    def __eq__(self, other):
        return self.value == other


def fill_columns(csv_map, prefix_map, depth):
    """
    Fill all columns of csv_map not present in prefix_map that have a depth less than the given
    :param csv_map:
    :param prefix_map:
    :param depth:
    :return:
    """
    for key in [key for key in csv_map if key not in prefix_map]:
        col = csv_map[key]
        col_depth = len(col)
        for _ in range(depth.value - col_depth):
            col.append(None)


def format_leaf_name(src_name, leaf_name):
    """
    Given a source name and a leaf name, format the leaf's name.
    :param src_name:
    :param leaf_name:
    :return:
    """
    if len(src_name) > 0:
        return src_name + '_' + leaf_name
    else:
        return leaf_name


def insert_list(csv_map, header, name, value, max_depth, current_depth):
    """
    Insert a list (array in json) into csv_map
    :param csv_map:
    :param header:
    :param name:
    :param value:
    :param max_depth:
    :param current_depth:
    :return: None
    """
    if len(value) == 0:
        return
    undef = []
    # Get the current row by popping everything at the current depth
    prefix_row = {}
    for key, col in csv_map.items():
        if len(col) - 1 == max_depth:
            prefix_row[key] = col.pop()
    for v in value:
        # Update the row to have all the elements saved for this row.
        for key in prefix_row:
            csv_map[key].append(prefix_row[key])

        # Save the current depth and then insert this value, while recording any columns that
        # could not be added due to being empty.
        pre_insertion_max_depth = max_depth.value
        insertion_result = insert_value(csv_map, header, '', name, v, max_depth, current_depth)

        # If the max_depth increased during the insertion, then subtract one because this line's prefix
        # has been counted twice.
        if pre_insertion_max_depth < max_depth.value:
            max_depth -= 1

        # If we got back a valid list of undefined columns, store them
        if insertion_result is not None and isinstance(insertion_result, type(tuple())):
            undef += [col for col in insertion_result[0] if col not in undef]
            undef = [col for col in undef if col not in insertion_result[1]]

        # Fill any columns that did not get a value from this insertion
        max_depth += 1
        current_depth += 1
        fill_columns(csv_map, prefix_row, max_depth)

    # For each column that never got a value, add them and fill them with null values
    for key in [key for key in undef if key not in csv_map]:
        csv_map[format_leaf_name(name, key)] = [None for _ in range(max_depth.value)]


def insert_dict(csv_map, header, name, value, max_depth, current_depth):
    """
    Insert a dict (object in json) into csv_map
    :param csv_map:
    :param header:
    :param name:
    :param value:
    :param max_depth:
    :param current_depth:
    :return: a tuple of two lists (unadded_cols, recursive_cols)
    where unadded_cols = list of columns that could not be added due to having null values,
    recursive_cols = names of columns that were not added due to the fact that they are recursive (list or dict)
    """
    leaves, dicts, lists = [], [], []

    # Sort the values of the given dict into their appropriate list while keeping track of the order they were received
    index = 0
    for k, v in value.items():
        if isinstance(v, type(dict())):
            dicts.append((k, v, header.insert_tree(index, k)))
        elif isinstance(v, type(list())):
            dicts.append((k, v, header.insert_tree(index, k)))
        else:
            leaves.append((k, v))
            header.insert_leaf(index, format_leaf_name(name, k))
        index += 1

    # Insert them in order: leaves, then dicts, then lists
    unadded_cols = []
    recursive_cols = []
    for k, v in leaves:
        if not insert_value(csv_map, header, name, k, v, max_depth, current_depth):
            # If this leaf could not be inserted due to having null values, keep track of it
            # in case it never gets a non-null value so that an empty column can be added manually later.
            unadded_cols.append(format_leaf_name(name, k))

    for k, v, t in dicts + lists:
        # keep track of the names of lists and dictionaries
        recursive_cols.append(format_leaf_name(name, k))
        insertion_results = insert_value(csv_map, t, name, k, v, max_depth, current_depth)
        if insertion_results is not None:
            # Add the results of this insertion to the ongoing lists
            unadded_cols += [col for col in insertion_results[0] if col not in unadded_cols]
            recursive_cols += [col for col in insertion_results[1] if col not in recursive_cols]

    return unadded_cols, recursive_cols


def insert_leaf(csv_map, name, value, max_depth, current_depth):
    """
    Insert a leaf to its proper csv column, creating it if it does not exist.
    Note that this function will not add a new column if the associated value is None,
    because there's a chance that it may be revealed to be a recursive column in the future.
    :param csv_map:
    :param name:
    :param value:
    :param max_depth:
    :param current_depth:
    :return: True if the given value was added, False otherwise.
    """
    if name not in csv_map:
        if value is None:
            # If this is the first time encountering this name, but its value is None, there is no way to know if it
            # is truly a leaf, so abort.
            return False
        csv_map[name] = [None for _ in range(current_depth)]
        for _ in range(max(max_depth.value - current_depth, 1)):
            csv_map[name].append(value)
    else:
        csv_map[name].append(value)
    return True


def insert_value(csv_map, header, src_name, name, value, max_depth, current_depth):
    """
    Given some value, properly add it to the csv_map.
    :param csv_map:
    :param header:
    :param src_name:
    :param name:
    :param value:
    :param max_depth:
    :param current_depth:
    :return: If :param value is an instance of:
        dict: See return type of insert_dict
        list: None
        leaf: See return type of insert_leaf
    """
    if isinstance(value, type(dict())):
        # Insert the dictionary and return the undefined columns that could not be added
        return insert_dict(csv_map, header, name, value, max_depth, current_depth)
    elif isinstance(value, type(list())):
        insert_list(csv_map, header, name, value, max_depth, current_depth)
        # Lists do not directly add columns, so returning a boolean would not make sense
        return None
    else:
        # Insert and return a boolean indicating whether or not the column could be added
        return insert_leaf(csv_map, format_leaf_name(src_name, name), value, max_depth, current_depth)


def convert_json_to_csv(input_path, output_path, filter):
    with open(input_path, 'r') as f:

        # Format json as much as possible if it's improperly formatted, while applying the filter (if any)
        data = json.load(f)
        if isinstance(data, type(dict())):
            if filter is not None:
                data = data[filter]
            if isinstance(data, type(dict())):
                data = [data]
        elif filter is not None:
            data = [row[filter] for row in data]

        csv_filename = output_path
        if not csv_filename.endswith('.csv'):
            csv_filename += '.csv'

        with open(csv_filename, 'w+', newline='') as csv_file:

            csv_map = OrderedDict()
            header = HeaderTree('')
            insert_list(csv_map, header, '', data, MutableInt(), 0)

            # Filter out un-added columns and sort the columns by the order they come in the json
            header = header.flatten()
            header = [col for col in header if col in csv_map]
            columns = [csv_map[key] for key in header]
            if len(columns) == 0:
                return

            csv_len = len(columns[0])
            max_len = max([len(col) for col in columns])
            min_len = min([len(col) for col in columns])
            if min_len < max_len:
                print('Error parsing JSON, make sure it\'s properly formatted. %s rows will be omitted.' % str(max_len - min_len))
                csv_len = min_len

            writer = csv.writer(csv_file)
            writer.writerow(header)
            for i in range(csv_len):
                row = [col[i] for col in columns]
                writer.writerow(row)


def parse_arguments(argv):
    args = []
    if len(argv) == 5 or len(argv) > 6 or len(argv) < 4 or argv[0] != '-in1' or argv[2] != '-out1':
        raise ValueError('Invalid arguments: arguments must be in the form -in1 <input path> -out1 <output path> [-filter <filter>]')
    args.append(argv[1])
    args.append(argv[3])
    if len(argv) >= 6:
        if argv[4] != '-filter':
            raise ValueError('Invalid flag: ' + argv[4])
        args.append(argv[5])
    return args


args = parse_arguments(sys.argv[1:])
convert_json_to_csv(args[0], args[1], args[2] if len(args) > 2 else None)
