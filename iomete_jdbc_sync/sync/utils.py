import re


def table_name_extractor(table):
    subquery_table = re.search(r"\([\S\s]*\)\s+(as)?\s*(\w+)", table.strip())
    if subquery_table:
        return subquery_table.group(2)
    return table
