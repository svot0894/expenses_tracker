from collections.abc import Callable


def not_null(column: str) -> str:
    """check if a row is not null"""
    return f"{column} IS NULL"


VALIDATIONS: dict[str, Callable[[str], str]] = {
    "not_null": not_null,
}