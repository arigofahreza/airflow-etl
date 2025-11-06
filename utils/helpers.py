import numpy as np


def validate_row(row):
    required_fields = ["date", "client_code", "client_type", "amount", "number"]
    return all(row.get(col) not in ("", None, np.nan) for col in required_fields)
