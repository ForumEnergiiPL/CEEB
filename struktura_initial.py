from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

REPORT_XLSX_URL = (
    "https://zone.gunb.gov.pl/sites/default/files/jasper_reports/"
    "strukutra_zrodel_ciepla_polska.xlsx"
)

RAW_XLSX_PATH = Path("zone_struktura_zrodel_ciepla_polska.xlsx")
OUT_CSV_PATH = Path("zone_struktura_zrodel_ciepla_polska.csv")


def download_file(url: str, timeout: int = 60) -> bytes:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (compatible; ZONE-CEEB-downloader/1.0; +https://zone.gunb.gov.pl)"
        )
    }
    response = requests.get(url, headers=headers, timeout=timeout)
    response.raise_for_status()
    return response.content


def extract_source_date(raw: pd.DataFrame) -> str | None:
    """
    Szuka w arkuszu wiersza z informacją 'Dane pozyskane z dnia'
    i zwraca datę w formacie YYYY-MM-DD.
    """
    mask = raw.apply(
        lambda row: row.astype(str).str.contains(
            "Dane pozyskane z dnia", case=False, na=False
        ).any(),
        axis=1,
    )

    if not mask.any():
        return None

    row = raw.loc[mask.idxmax()]

    for value in row.tolist():
        if isinstance(value, (pd.Timestamp, datetime)):
            return pd.Timestamp(value).date().isoformat()

        if isinstance(value, str):
            value = value.strip()
            for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%d-%m-%Y"):
                try:
                    return datetime.strptime(value, fmt).date().isoformat()
                except ValueError:
                    pass

    return None


def parse_zone_excel(excel_bytes: bytes) -> pd.DataFrame:
    """
    Czyta arkusz ZONE i zwraca oczyszczoną tabelę:
    data_pozyskania, zrodlo_ciepla, liczba_zrodel_ciepla, udzial_procentowy
    """
    raw = pd.read_excel(
        io.BytesIO(excel_bytes),
        sheet_name=0,        # pierwszy arkusz
        header=None,
        engine="openpyxl",
    )

    # Usunięcie pustych wierszy i kolumn
    raw = raw.dropna(axis=0, how="all").dropna(axis=1, how="all").reset_index(drop=True)

    source_date = extract_source_date(raw)

    # Szukamy wiersza nagłówków
    header_mask = raw.apply(
        lambda row: (
            row.astype(str).str.contains("Źródło ciepła", case=False, na=False).any()
            and row.astype(str).str.contains("Liczba źródeł ciepła", case=False, na=False).any()
        ),
        axis=1,
    )

    if not header_mask.any():
        raise ValueError("Nie znaleziono wiersza nagłówków w pliku XLSX.")

    header_idx = header_mask.idxmax()
    header_row = raw.loc[header_idx]

    def find_col(pattern: str):
        matches = header_row[header_row.astype(str).str.contains(pattern, case=False, na=False)]
        if matches.empty:
            raise ValueError(f"Nie znaleziono kolumny pasującej do wzorca: {pattern}")
        return matches.index[0]

    source_col = find_col(r"Źródło ciepła")
    count_col = find_col(r"Liczba źródeł ciepła")
    share_col = find_col(r"Udział procentowy")

    data = raw.loc[header_idx + 1 :, [source_col, count_col, share_col]].copy()
    data.columns = ["zrodlo_ciepla", "liczba_zrodel_ciepla", "udzial_procentowy"]

    # Czyszczenie
    data["zrodlo_ciepla"] = data["zrodlo_ciepla"].astype("string").str.strip()

    data = data[data["zrodlo_ciepla"].notna()]
    data = data[~data["zrodlo_ciepla"].str.fullmatch(r"Suma:?", na=False)]
    data = data[~data["zrodlo_ciepla"].str.contains("Dane pozyskane", case=False, na=False)]

    data["liczba_zrodel_ciepla"] = pd.to_numeric(
        data["liczba_zrodel_ciepla"], errors="coerce"
    ).astype("Int64")

    data["udzial_procentowy"] = (
        data["udzial_procentowy"]
        .astype("string")
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip()
    )
    data["udzial_procentowy"] = pd.to_numeric(data["udzial_procentowy"], errors="coerce")

    # Dodaj datę pozyskania do każdej obserwacji
    if source_date:
        data.insert(0, "data_pozyskania", source_date)

    return data.reset_index(drop=True)


def main() -> None:
    excel_bytes = download_file(REPORT_XLSX_URL)

    # Zapis surowego pliku XLSX do audytu / archiwizacji
    RAW_XLSX_PATH.write_bytes(excel_bytes)

    df = parse_zone_excel(excel_bytes)

    # UTF-8-SIG ułatwia otwieranie w Excelu na Windows
    df.to_csv(OUT_CSV_PATH, index=False, encoding="utf-8-sig")

    print(f"Zapisano XLSX: {RAW_XLSX_PATH.resolve()}")
    print(f"Zapisano CSV : {OUT_CSV_PATH.resolve()}")
    print()
    print(df)


if __name__ == "__main__":
    main()
