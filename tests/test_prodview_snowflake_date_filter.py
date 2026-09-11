"""Snowflake pull uses inclusive calendar-date bounds on DTTM."""

from prodview_update_gui import _SF_DATE_RANGE_WHERE, _SF_QUERIES


def test_snowflake_queries_filter_on_cast_date_not_timestamp():
    assert "CAST(DTTM AS DATE)" in _SF_DATE_RANGE_WHERE
    assert "DTTM <=" not in _SF_DATE_RANGE_WHERE.replace("CAST(DTTM AS DATE)", "")
    for _name, (_id_col, sql) in _SF_QUERIES.items():
        assert _SF_DATE_RANGE_WHERE in sql, _name
