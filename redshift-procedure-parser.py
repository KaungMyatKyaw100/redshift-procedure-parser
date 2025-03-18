def extract_tables_from_redshift_procedure(procedure_source, preserve_case=False):
    """
    Extracts table names from a Redshift stored procedure definition.

    Args:
        procedure_source (str): The Redshift stored procedure definition.
        preserve_case (bool, optional): If True, preserves the original case of table names. Defaults to False.

    Returns:
        list: A list of table names.
    """
    procedure_source = re.sub(r'--.*$', '', procedure_source, flags=re.MULTILINE)
    procedure_source = re.sub(r'/\*.*?\*/', '', procedure_source, flags=re.DOTALL)

    # Improved regex to capture various table name patterns
    table_matches = re.findall(
        r'\b(FROM|JOIN|INSERT\s+INTO|UPDATE|DELETE\s+FROM)\s+("[a-zA-Z0-9_]+"\."[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|"[a-zA-Z0-9_]+"\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|[a-zA-Z_][a-zA-Z0-9_.]+)',
        procedure_source,
        re.IGNORECASE,
    )

    table_names = [match[1] for match in table_matches]

    if not preserve_case:
        table_names = [table.lower() for table in table_names]

    table_names = list(set(table_names))
    table_names = [table.replace('"', '') for table in table_names if "." in table]

    # Additional patterns based on experience
    # Capturing table names in CREATE TABLE AS statements
    create_table_matches = re.findall(
        r'\bCREATE\s+TABLE\s+("[a-zA-Z0-9_]+"\."[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|"[a-zA-Z0-9_]+"\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|[a-zA-Z_][a-zA-Z0-9_.]+)\s+AS',
        procedure_source,
        re.IGNORECASE,
    )

    create_table_names = [match for match in create_table_matches]

    if not preserve_case:
        create_table_names = [table.lower() for table in create_table_names]

    create_table_names = [table.replace('"', '') for table in create_table_names if "." in table]

    table_names.extend(create_table_names)

    # Added regex for implicit joins
    implicit_join_matches = re.findall(
        r'\bFROM\s+("[a-zA-Z0-9_]+"\."[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|"[a-zA-Z0-9_]+"\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|[a-zA-Z_][a-zA-Z0-9_.]+),\s*("[a-zA-Z0-9_]+"\."[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|"[a-zA-Z0-9_]+"\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|[a-zA-Z_][a-zA-Z0-9_.]+)',
        procedure_source,
        re.IGNORECASE,
    )
    implicit_join_matches_multiple_commas = re.findall(
        r'\bFROM\s+("[a-zA-Z0-9_]+"\."[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|"[a-zA-Z0-9_]+"\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|[a-zA-Z_][a-zA-Z0-9_.]+),\s*("[a-zA-Z0-9_]+"\."[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|"[a-zA-Z0-9_]+"\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|[a-zA-Z_][a-zA-Z0-9_.]+),\s*("[a-zA-Z0-9_]+"\."[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|"[a-zA-Z0-9_]+"\.[a-zA-Z0-9_]+|[a-zA-Z0-9_]+\."[a-zA-Z0-9_]+|[a-zA-Z_][a-zA-Z0-9_.]+)',
        procedure_source,
        re.IGNORECASE,
    )

    for match in implicit_join_matches:
        table_names.extend(match)
    for match in implicit_join_matches_multiple_commas:
        table_names.extend(match)

    # Handle dynamic table names
    dynamic_table_matches = re.findall(
        r"FROM\s+([a-zA-Z0-9_]+)\.'\s*\|\|\s*([a-zA-Z0-9_]+)",
        procedure_source,
        re.IGNORECASE,
    )

    dynamic_table_names = [f"{match[0]}.{match[1]} (by_parameter)" for match in dynamic_table_matches]

    if not preserve_case:
        dynamic_table_names = [name.lower() for name in dynamic_table_names]

    table_names.extend(dynamic_table_names)

    table_names = list(set(table_names))
    table_names = [table.replace('"', '') for table in table_names if "." in table]

    # Remove the schema name if it exists in the table_names
    dynamic_schema_names = [match[0] for match in dynamic_table_matches]

    #Remove the schema from the result if the dynamic table name is present.
    table_names = [name for name in table_names if name not in dynamic_schema_names]

    return table_names