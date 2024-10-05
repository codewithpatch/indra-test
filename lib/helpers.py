import re


def has_nested_node(dtype: str) -> bool:
    struct_count = len(re.findall(r"struct", dtype))

    return struct_count > 1
