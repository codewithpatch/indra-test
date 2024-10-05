from dataclasses import dataclass


@dataclass
class ColumnNodeHash:
    column_name: str
    hash_value: str