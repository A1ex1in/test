# НАСТРОЙКИ

ACCESS_DB     = fr"D:\code\prog1\all_20250730.mdb" # путь к Access (.accdb/.mdb)
ACCESS_TABLE  = "Config" # таблица с MCC/MNC/LAC/CI
MCC_COL       = "MCC"
MNC_COL       = "MNC"
LAC_COL       = "lac"
CI_COL        = "ci"
GCI_XLSX      = fr"D:\code\prog1\ADD_LAIGCI_20250731.xlsx" # Excel с колонкой GCI
GCI_SHEET     = None # None = авто (найдём на 1-м листе с колонкой)
GCI_COLUMN    = "GCI"
SAI_XLSX      = fr"D:\code\prog1\ADD_LAISAI_20250731.xlsx" # Excel с колонкой SAI
SAI_SHEET     = None
SAI_COLUMN    = "SAI"
# форматирование идентификатора из Access
MNC_MODE      = "auto" # "auto" (2/3 цифры), либо "2" или "3"
LAC_HEX_WIDTH = 4 # 4 = типично для GSM LAC
CI_HEX_WIDTH  = 4 # 4 = типично для GSM CI (можно 6)
# сохранить NGCI/NSAI в CSV (или None, чтобы не сохранять)
SAVE_CSV      = fr".\diff2.csv"

# ---------------------------------------------------------------------------------------
import re
from pathlib import Path

import pandas as pd
import pyodbc


def connect_access(db_path: Path):
    conn_str = (r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" fr"DBQ={db_path};")
    try:
        return pyodbc.connect(conn_str)
    except pyodbc.Error as e:
        raise SystemExit("Не удалось подключиться к Access.\n"f"ODBC ошибка: {e}")


def _digits(s):
    if pd.isna(s):
        return ""
    return re.sub(r"\s+", "", str(s)).strip()


def _to_hex(val, width: int):
    if pd.isna(val) or str(val).strip() == "":
        return ""
    try:
        ival = int(val)
    except Exception:
        ival = int(float(str(val).replace(",", ".")))
    return f"{ival:0{width}X}"


def build_id(mcc, mnc, lac, ci, mnc_mode="auto", lac_w=4, ci_w=4):
    mcc_s = _digits(mcc).zfill(3)  # MCC почти всегда 3 цифры

    mnc_raw = _digits(mnc)
    try:
        mnc_i = int(mnc_raw)
    except Exception:
        mnc_i = None

    if mnc_mode == "2":
        mnc_s = mnc_raw.zfill(2)
    elif mnc_mode == "3":
        mnc_s = mnc_raw.zfill(3)
    else:
        # auto: <100 -> 2 цифры, иначе 3
        if mnc_i is not None:
            mnc_s = f"{mnc_i:02d}" if mnc_i < 100 else f"{mnc_i:03d}"
        else:
            mnc_s = mnc_raw  # если не число — оставим как есть

    lac_hex = _to_hex(lac, lac_w)
    ci_hex  = _to_hex(ci,  ci_w)
    return f"{mcc_s}{mnc_s}{lac_hex}{ci_hex}".upper()


def fetch_access_ids(db_path: Path, table: str, mcc_col: str, mnc_col: str, lac_col: str, ci_col: str):
    conn = connect_access(db_path)
    try:
        sql = f"SELECT {mcc_col}, {mnc_col}, {lac_col}, {ci_col} FROM {table}"
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        ids = []
        for r in rows:
            mcc, mnc, lac, ci = r[0], r[1], r[2], r[3]
            ids.append(build_id(mcc, mnc, lac, ci, MNC_MODE, LAC_HEX_WIDTH, CI_HEX_WIDTH))
        return [s for s in ids if s]  # убрать пустые
    finally:
        conn.close()


def _normalize_colnames(cols):
    # нормализация имён: убрать пробелы, нижний регистр
    return {re.sub(r"\s+", "", str(c)).strip().lower(): c for c in cols}


def read_excel_column(path: Path, wanted_col: str, sheet=None):
    """
    Если sheet=None, пытаемся:
      1) прочитать первый лист (sheet_name=0) и найти колонку;
      2) если не нашли — читаем все листы (sheet_name=None) и ищем колонку на первом подходящем.
    Возвращаем список строк (UPPER, без пробелов).
    """
    wanted_key = re.sub(r"\s+", "", wanted_col).strip().lower()

    def _extract(df: pd.DataFrame) -> list[str]:
        mapping = _normalize_colnames(df.columns)
        if wanted_key not in mapping:
            # частичное совпадение (напр. "GCI" встречается как "GCI ID")
            for k, orig in mapping.items():
                if wanted_key in k or k in wanted_key:
                    colname = orig
                    break
            else:
                raise KeyError
        else:
            colname = mapping[wanted_key]

        ser = df[colname].dropna().astype(str).map(lambda x: x.strip().upper())
        ser = ser.str.replace(r"\s+", "", regex=True)
        return ser.tolist()

    # Явно заданный лист
    if sheet is not None:
        df = pd.read_excel(path, sheet_name=sheet)
        return _extract(df)

    # Пробуем 1-й лист
    try:
        df = pd.read_excel(path, sheet_name=0)
        return _extract(df)
    except Exception:
        pass

    # Ищем по всем листам
    all_sheets = pd.read_excel(path, sheet_name=None)
    tried = {}
    for name, df in all_sheets.items():
        try:
            return _extract(df)
        except Exception:
            tried[name] = [str(c) for c in df.columns]

    detail = "; ".join(f"{k}: {v}" for k, v in tried.items())
    raise SystemExit(f"Колонка '{wanted_col}' не найдена ни на одном листе файла '{path.name}'. "
                     f"Проверены листы и их столбцы: {detail}")


def main():
    # 1) Собираем var1 из Access: MCC+MNC+LACHEX+CIHEX
    var1 = fetch_access_ids(Path(ACCESS_DB), ACCESS_TABLE, MCC_COL, MNC_COL, LAC_COL, CI_COL)

    # 2) Читаем GCI из Excel
    var2 = read_excel_column(Path(GCI_XLSX), GCI_COLUMN, GCI_SHEET)

    # 3) Читаем SAI из Excel
    var3 = read_excel_column(Path(SAI_XLSX), SAI_COLUMN, SAI_SHEET)

    # 4) Сравнение
    s1 = set(var1)
    NGCI = [g for g in var2 if g not in s1]
    NSAI = [s for s in var3 if s not in s1]

    # 5) Вывод
    print(f"[1] Access IDs (MCC+MNC+LACHEX+CIHEX): {len(var1)}")
    print(f"[2] GCI из Excel                      : {len(var2)}")
    print(f"[3] SAI из Excel                      : {len(var3)}")
    print(f"[GCI] не найдены в [1]                : {len(NGCI)}")
    print(f"[SAI] не найдены в [1]                : {len(NSAI)}")

    # Покажем первые несколько для контроля
    head_n = 10
    if NGCI:
        print("\nПримеры GCI:", NGCI[:head_n])
    if NSAI:
        print("\nПримеры SAI:", NSAI[:head_n])

    # 6) По желанию — сохранить в CSV
    if SAVE_CSV:
        out = Path(SAVE_CSV)
        pd.DataFrame({
            "GCI_not_in_Access": pd.Series(NGCI, dtype="string"),
            "SAI_not_in_Access": pd.Series(NSAI, dtype="string")
        }).to_csv(out, index=False, encoding="utf-8-sig")
        print(f"\nСохранено в: {out.resolve()}")
    # return var1, var2, var3, NGCI, NSAI


if __name__ == "__main__":

    main()
