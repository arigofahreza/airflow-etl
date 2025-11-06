def validate_row(row):
    required_fields = ["date", "client_code", "client_type", "amount", "stt_number"]
    return all(row.get(col) not in ("", None) for col in required_fields)
