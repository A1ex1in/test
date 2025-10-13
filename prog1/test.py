import argparse
from pathlib import Path
import re

import pandas as pd
import pyodbc


def connect_access(db_path: Path) -> pyodbc.Connection:
    conn_str = (
        r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};"
        fr"DBQ={db_path};"
    )
    try:
        return pyodbc.connect(conn_str, autocommit=False)
    except pyodbc.Error as e:
        msg = str(e)
        hint = ""
        if "Data source name not found" in msg or "Driver" in msg:
            hint = (
                "\nПохоже, нет ODBC-драйвера Access. "
                "Установите Microsoft Access Database Engine (x64 для 64-битного Python)."
            )
        raise RuntimeError(f"Не удалось подключиться к Access: {msg}{hint}")


def norm_digits(val, min_width: int = None, max_width: int = None) -> str:
    """Аккуратная нормализация числового поля в строку без пробелов."""
    if pd.isna(val):
        return ""
    s = str(int(val)) if isinstance(val, (int, float)) and not pd.isna(val) else str(val).strip()
    s = re.sub(r"\s+", "", s)
    if min_width:
        s = s.zfill(min_width) if len(s) < min_width else s
    if max_width and len(s) > max_width:
        # Если внезапно длиннее ожидаемого — оставим как есть, но можно обрезать/ошибку по желанию
        pass
    return s


def to_hex(val, width: int) -> str:
    if pd.isna(val):
        return ""
    try:
        ival = int(val)
    except Exception:
        ival = int(float(val))
    # безопасный диапазон: LAC/CI часто 0..65535 (GSM). По умолчанию делаем 4 символа HEX.
    return f"{ival:0{width}X}"


def build_id(mcc, mnc, lac, ci, *, mcc_width=3, mnc_mode="auto", lac_width=4, ci_width=4) -> str:
    """
    mnc_mode:
      - 'auto'  : 2 цифры для <100, 3 цифры для >=100 (типично для 2/3-значного MNC)
      - '2'     : всегда 2 цифры
      - '3'     : всегда 3 цифры
    """
    # MCC почти всегда 3-значный
    mcc_str = norm_digits(mcc, min_width=mcc_width, max_width=mcc_width) if mcc_width else norm_digits(mcc)

    # MNC: авто (2 или 3), либо фиксировано
    raw_mnc = norm_digits(mnc)
    if mnc_mode == "2":
        mnc_str = raw_mnc.zfill(2)
    elif mnc_mode == "3":
        mnc_str = raw_mnc.zfill(3)
    else:
        # auto: если >=100 → 3, иначе → 2
        try:
            mnc_int = int(raw_mnc)
            mnc_str = f"{mnc_int:03d}" if mnc_int >= 100 else f"{mnc_int:02d}"
        except Exception:
            # если вдруг не число — оставим как есть
            mnc_str = raw_mnc

    lac_hex = to_hex(lac, width=lac_width)
    ci_hex  = to_hex(ci,  width=ci_width)
    return f"{mcc_str}{mnc_str}{lac_hex}{ci_hex}".upper()


def load_access_compound_ids(db_path: Path, table: str, *, mcc_col="MCC", mnc_col="MNC",
                             lac_col="lac", ci_col="ci",
                             mnc_mode="auto", lac_width=4, ci_width=4) -> list[str]:
    conn = connect_access(db_path)
    try:
        sql = f"SELECT {mcc_col}, {mnc_col}, {lac_col}, {ci_col} FROM {table}"
        rows = pd.read_sql(sql, conn)
    finally:
        conn.close()

    ids = []
    for _, r in rows.iterrows():
        s = build_id(
            r[mcc_col], r[mnc_col], r[lac_col], r[ci_col],
            mcc_width=3, mnc_mode=mnc_mode, lac_width=lac_width, ci_width=ci_width
        )
        if s:
            ids.append(s)
    return ids


def load_excel_column(path: Path, column: str, sheet: str | int | None = None, header: int = 0) -> list[str]:
    import pandas as pd, re

    def _normalize(s: str) -> str:
        return re.sub(r"\s+", "", str(s)).strip().lower()

    def _resolve_column(df: pd.DataFrame, target: str) -> str:
        target_norm = _normalize(target)
        mapping = {_normalize(c): c for c in df.columns}
        if target_norm in mapping:
            return mapping[target_norm]
        # Фолбэк: частичное совпадение
        for k, orig in mapping.items():
            if target_norm in k or k in target_norm:
                return orig
        raise KeyError(f"Столбец '{target}' не найден. Есть: {list(df.columns)}")

    obj = pd.read_excel(path, sheet_name=sheet, header=header)

    def _extract_from_df(df: pd.DataFrame) -> list[str]:
        col = _resolve_column(df, column)
        series = df[col].dropna().astype(str).map(lambda x: x.strip().upper())
        series = series.str.replace(r"\s+", "", regex=True)
        return series.tolist()

    if isinstance(obj, dict):
        # Ищем первый лист, где есть нужная колонка
        tried = {}
        for name, df in obj.items():
            try:
                return _extract_from_df(df)
            except KeyError as e:
                tried[name] = [str(c) for c in df.columns]
        # Если не нашли — даём подробную ошибку
        details = "; ".join(f"{k}: {v}" for k, v in tried.items())
        raise KeyError(f"Столбец '{column}' не найден ни на одном листе. Проверены листы и их столбцы: {details}")
    else:
        return _extract_from_df(obj)


def main():
    ap = argparse.ArgumentParser(description="Сбор и сравнение MCC+MNC+LACHEX+CIHEX с GCI/SAI.")
    ap.add_argument("--access",         required=True, help="Путь к .mdb/.accdb")
    ap.add_argument("--table",          default="Config", help="Имя таблицы в Access (по умолчанию Config)")
    ap.add_argument("--mcc-col",        default="MCC")
    ap.add_argument("--mnc-col",        default="MNC")
    ap.add_argument("--lac-col",        default="lac")
    ap.add_argument("--ci-col",         default="ci")

    ap.add_argument("--mnc-mode",       choices=["auto", "2", "3"], default="auto",
                    help="Длина MNC: auto (2/3), либо фиксированно 2 или 3")
    ap.add_argument("--lac-width",      type=int, default=4, help="HEX ширина для LAC (по умолчанию 4)")
    ap.add_argument("--ci-width",       type=int, default=4, help="HEX ширина для CI  (по умолчанию 4)")

    ap.add_argument("--gci-xlsx",       required=True, help="Excel с колонкой GCI")
    ap.add_argument("--gci-column",     default="GCI")
    ap.add_argument("--gci-sheet",      default=None, help="Имя листа (если нужно)")

    ap.add_argument("--sai-xlsx",       required=True, help="Excel с колонкой SAI")
    ap.add_argument("--sai-column",     default="SAI")
    ap.add_argument("--sai-sheet",      default=None, help="Имя листа (если нужно)")

    ap.add_argument("--save-csv",       default=None, help="Если указать путь, сохранит NGCI/NSAI в CSV")

    args = ap.parse_args()

    access_ids = load_access_compound_ids(
        Path(args.access), args.table,
        mcc_col=args.mcc_col, mnc_col=args.mnc_col, lac_col=args.lac_col, ci_col=args.ci_col,
        mnc_mode=args.mnc_mode, lac_width=args.lac_width, ci_width=args.ci_width
    )
    var1 = [s.upper() for s in access_ids]         # 1: MCC+MNC+lacHEX+ciHEX
    var2 = load_excel_column(Path(args.gci_xlsx), args.gci_column, args.gci_sheet)  # 2: GCI
    var3 = load_excel_column(Path(args.sai_xlsx), args.sai_column, args.sai_sheet)  # 3: SAI

    set1 = set(var1)
    NGCI = [g for g in var2 if g not in set1]
    NSAI = [s for s in var3 if s not in set1]

    # Вывод
    print(f"[1] Сформировано из Access: {len(var1)} записей")
    print(f"[2] Загружено GCI         : {len(var2)} записей")
    print(f"[3] Загружено SAI         : {len(var3)} записей")
    print(f"[NGCI] несовпадений       : {len(NGCI)}")
    print(f"[NSAI] несовпадений       : {len(NSAI)}")

    # Если нужно — сохраняем
    if args.save_csv:
        out = Path(args.save_csv)
        df_out = pd.DataFrame({
            "NGCI_not_in_Access": pd.Series(NGCI, dtype="string"),
            "NSAI_not_in_Access": pd.Series(NSAI, dtype="string")
        })
        df_out.to_csv(out, index=False, encoding="utf-8-sig")
        print(f"Сохранено в: {out.resolve()}")

    # Если сценарий используется как модуль, полезно вернуть переменные:
    # var1, var2, var3, NGCI, NSAI
    # Но в режиме CLI просто печатаем.


if __name__ == "__main__":
    main()
