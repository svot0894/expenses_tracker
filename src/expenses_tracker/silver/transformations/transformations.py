
from collections.abc import Callable

def quote_identifier(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def trim_text(column: str) -> str:
    return f"NULLIF(TRIM({quote_identifier(column)}), '')"


def parse_date(column: str, date_format: str) -> str:
    value = trim_text(column)
    return f"TRY_STRPTIME({value}, '{date_format}')::DATE"


def parse_decimal(
    column: str,
    precision: int = 18,
    scale: int = 2,
) -> str:
    value = quote_identifier(column)

    if not (1 <= precision <= 38 and 0 <= scale <= precision):
        raise ValueError("Invalid decimal precision or scale")

    return (
        f"TRY_CAST({value} "
        f"AS DECIMAL({precision}, {scale}))"
    )

def signed_amount(debit_column: str, credit_column: str) -> str:
    """For cases where values are split between Debit and Credit it joins them as a single column."""
    debit_value = parse_decimal(debit_column)
    credit_value = parse_decimal(credit_column)
    return f"COALESCE({debit_value}, {credit_value})"


TRANSFORMATIONS: dict[str, Callable[..., str]] = {
    "trim_text": trim_text,
    "parse_date": parse_date,
    "parse_decimal": parse_decimal,
    "signed_amount": signed_amount,
}