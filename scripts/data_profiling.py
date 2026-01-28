import pandas as pd
from ydata_profiling import ProfileReport
import os

def generate_profile_report(table_name, query, output_path, conn):
    """
    Generate a data profiling report for a given table/query.
    """
    df = pd.read_sql(query, conn)
    profile = ProfileReport(df, title=f"Data Profile for {table_name}", explorative=True)
    profile.to_file(output_path)
    print(f"Profile report saved to {output_path}")

# Example usage in Airflow task
if __name__ == "__main__":
    # This would be called from Airflow with actual connection
    pass