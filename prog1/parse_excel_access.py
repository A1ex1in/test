import os
from typing import Optional
import pandas as pd
import pyodbc


def dec_to_hex_str(value: int, width: Optional[int] = None) -> str:
    hex_str = format(int(value), 'X')
    if width is not None:
        return hex_str.zfill(width)
    return hex_str

def read_excel_column(excel_path: str) -> pd.DataFrame:
    df = pd.read_excel(excel_path, sheet_name=0)
    df_columns_lower = {c.lower(): c for c in df.columns}
    if 'gsi' in df_columns_lower:
        col_name = df_columns_lower['gsi']
    elif 'sai' in df_columns_lower:
        col_name = df_columns_lower['sai']
    else:
        raise ValueError(f"Excel file {excel_path} must contain a 'gsi' or 'sai' column")
    return df[[col_name]].rename(columns={col_name: 'gsi'})

def read_access_table(access_path: str, table_name: str) -> pd.DataFrame:
    conn_str = (r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"f"Dbq={access_path};")
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        query = f"SELECT mcc, mnc, lac, ci FROM {table_name}"
        df = pd.read_sql(query, conn)
    return df

def combine_dataframes(excel_df: pd.DataFrame,access_df: pd.DataFrame,lac_width: Optional[int] = None,ci_width: Optional[int] = None,mcc_width: int = 3,mnc_width: int = 3,) -> pd.DataFrame:
    access_df = access_df.copy()
    access_df['lac_hex'] = access_df['lac'].apply(lambda x: dec_to_hex_str(x, width=lac_width))
    access_df['ci_hex'] = access_df['ci'].apply(lambda x: dec_to_hex_str(x, width=ci_width))
    access_df['mcc_str'] = access_df['mcc'].astype(str).str.zfill(mcc_width)
    access_df['mnc_str'] = access_df['mnc'].astype(str).str.zfill(mnc_width)
    access_df['combined'] = (access_df['mcc_str'] + access_df['mnc_str'] + access_df['lac_hex'] + access_df['ci_hex'])
    min_len = min(len(excel_df), len(access_df))
    combined_df = pd.concat([excel_df.iloc[:min_len].reset_index(drop=True), access_df.iloc[:min_len].reset_index(drop=True)], axis=1,)
    return combined_df

def main() -> None:
    excel_path = os.path.join(os.getcwd(), 'your_excel_file.xlsx')
    access_path = os.path.join(os.getcwd(), 'your_access_db.accdb')
    table_name = 'your_table_name'
    excel_df = read_excel_column(excel_path)
    access_df = read_access_table(access_path, table_name)
    combined_df = combine_dataframes(excel_df=excel_df,access_df=access_df,lac_width=None,ci_width=None,mcc_width=3,mnc_width=3,)
    output_path = os.path.join(os.getcwd(), 'combined_output.csv')
    combined_df.to_csv(output_path, index=False)
    print('Combined data saved to:', output_path)
    print(combined_df.head())

if __name__ == '__main__':
    main()