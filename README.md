# Extract Tables from Redshift Stored Procedures

## Overview
This Python script extracts table names from a given Amazon Redshift stored procedure. It scans SQL statements and returns a list of tables referenced in `FROM`, `JOIN`, `INSERT INTO`, `UPDATE`, `DELETE FROM`, and `CREATE TABLE AS` statements.

## Features
- Removes comments (`--` and `/* */`) to avoid false matches.
- Extracts table names from different SQL statements.
- Identifies dynamically generated table names.
- Supports implicit joins (tables separated by commas in `FROM` clauses).
- Allows case preservation (optional).

## Installation
No external dependencies are required. Ensure you have Python 3 installed.

## Usage
### Function Definition
```python
extract_tables_from_redshift_procedure(procedure_source: str, preserve_case: bool = False) -> list
```

### Parameters
- `procedure_source` (str): The Redshift stored procedure definition.
- `preserve_case` (bool, optional): If `True`, preserves table name case. Defaults to `False`.

### Returns
- `list`: A list of extracted table names.

### Example Usage
```python
procedure_source = """
    CREATE OR REPLACE PROCEDURE sample_procedure()
    LANGUAGE plpgsql
    AS $$
    BEGIN
        INSERT INTO schema1.table1 SELECT * FROM schema2.table2;
        UPDATE schema3.table3 SET column1 = 'value' WHERE id IN (SELECT id FROM schema4.table4);
    END;
    $$;
"""

tables = extract_tables_from_redshift_procedure(procedure_source)
print(tables)
```
#### Output
```
['schema1.table1', 'schema2.table2', 'schema3.table3', 'schema4.table4']
```

## Contributing
Contributions are welcome! Feel free to submit pull requests for improvements or additional features.

## License
This project is licensed under the MIT License.

