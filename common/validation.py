def validate_columns(job_name, source_columns, destination_columns, primary_keys):
    destination_lookup = {
        column.lower(): column
        for column in destination_columns
    }

    matched_columns = []

    for source_column in source_columns:
        source_key = source_column.lower()

        if source_key in destination_lookup:
            matched_columns.append(destination_lookup[source_key])

    matched_lookup = {
        column.lower()
        for column in matched_columns
    }

    missing_primary_keys = [
        key for key in primary_keys
        if key.lower() not in matched_lookup
    ]

    if missing_primary_keys:
        raise RuntimeError(
            f"[{job_name}] Missing primary key column(s): "
            + ", ".join(missing_primary_keys)
        )

    if not matched_columns:
        raise RuntimeError(
            f"[{job_name}] No matching columns found between source file and destination table."
        )

    ignored_columns = [
        column for column in source_columns
        if column.lower() not in destination_lookup
    ]

    if ignored_columns:
        print(f"[{job_name}] Warning: these source columns are not in destination table:")
        for column in ignored_columns:
            print(f"  - {column}")

    return matched_columns