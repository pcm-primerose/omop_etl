import polars as pl


def date_parser_helper(col_name: pl.col) -> pl.col:
    """Safely parses missing dates using polars expressions API, given col name containing dates"""
    col_expr = pl.col(col_name) if isinstance(col_name, str) else col_name

    return pl.when(col_expr.str.len_chars() == 4).then(col_expr + "-01-01").when(col_expr.str.len_chars() == 7).then(col_expr + "-01").otherwise(col_expr)


def _na_to_none(value):
    return None if value == "NA" else value


def _safe_convert_to_int(value):
    try:
        return int(value)
    except (ValueError, TypeError):
        return value
