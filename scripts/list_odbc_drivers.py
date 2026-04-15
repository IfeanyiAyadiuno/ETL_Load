#!/usr/bin/env python3
"""List ODBC drivers installed on this machine (setup / IT diagnostic)."""
import pyodbc

if __name__ == "__main__":
    for name in pyodbc.drivers():
        print(name)
