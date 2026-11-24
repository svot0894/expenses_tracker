from collections.abc import Callable


def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def trim_text(column: str) -> str:
    return f"NULLIF(TRIM({quote_identifier(column)}), '')"


def parse_date(column: str, date_format: str) -> str:
    value = trim_text(column)
    return f"TRY_STRPTIME({value}, '{date_format}')::DATE"


def normalize_numbers(column: str) -> str:
    decimal_regex = r"[-+]?[0-9]{1,3}(,[0-9]{3})+(\.[0-9]+)?"

    return (
        f"CASE WHEN regexp_full_match({column}, '{decimal_regex}') "
        f"THEN replace({column}, ',', '') "
        f"ELSE {column} END "
    )


def parse_decimal(
    column: str,
    precision: int = 18,
    scale: int = 2,
) -> str:
    value = quote_identifier(column)

    if not (1 <= precision <= 38 and 0 <= scale <= precision):
        raise ValueError("Invalid decimal precision or scale")

    # handle different numerical formats

    return (
        f"TRY_CAST({value} "
        f"AS DECIMAL({precision}, {scale}))"
    )


def combine_columns(columns: list[str]) -> str:
    """Coalesce and parse multiple source columns into one single column."""
    if not columns:
        raise ValueError("At least one column is required.")

    expressions = ", ".join(quote_identifier(c) for c in columns)

    return f"COALESCE({expressions})"



def generate_id(**column: str) -> str:
    return "UUID()"


TRANSFORMATIONS: dict[str, Callable[..., str]] = {
    "trim_text": trim_text,
    "parse_date": parse_date,
    "normalize_numbers": normalize_numbers,
    "parse_decimal": parse_decimal,
    "combine_columns": combine_columns,
    "generate_id": generate_id,
}