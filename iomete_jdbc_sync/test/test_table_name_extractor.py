from sync.utils import table_name_extractor


def test_single_line():
    assert table_name_extractor(table="(SELECT * FROM salaries) salaries") == "salaries"


def test_single_line_with_as():
    assert table_name_extractor(table="(SELECT * FROM salaries) as salaries") == "salaries"


def test_multiline():
    assert table_name_extractor(table="""
    (SELECT * 
    FROM salaries)
    salaries
    """) == "salaries"


def test_multiline_with_as():
    assert table_name_extractor(table="""
    (SELECT * 
    FROM salaries)
    as salaries
    """) == "salaries"
