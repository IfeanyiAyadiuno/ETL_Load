from access_io import connect_access

def print_access_columns(table="CDA_Table"):
    conn = connect_access()
    cur = conn.cursor()

    cols = [row.column_name for row in cur.columns(table=table)]
    print("\n".join(cols))

    cur.close()
    conn.close()

if __name__ == "__main__":
    print_access_columns("CDA_Table")
