from __future__ import annotations

import io
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

# XLSX powiązany z raportem:
# https://zone.gunb.gov.pl/pl/raporty/struktura_budynkow_zrodla_co
REPORT_XLSX_URL = (
    "https://zone.gunb.gov.pl/sites/default/files/jasper_reports/"
    "struktura_polska.xlsx"
)

RAW_XLSX_PATH = Path("zone_struktura_budynkow_zrodla_co_polska.xlsx")
OUT_CSV_PATH = Path("zone_struktura_budynkow_zrodla_co_polska.csv")


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

    # najpierw próbujemy bezpośrednio z komórek
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

    # fallback: szukamy daty w całym złączonym wierszu
    row_text = " ".join(str(v) for v in row.tolist() if pd.notna(v)).strip()
    for token in row_text.replace("\xa0", " ").split():
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d.%m.%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(token.strip(), fmt).date().isoformat()
            except ValueError:
                pass

    return None


def parse_zone_excel(excel_bytes: bytes) -> pd.DataFrame:
    """
    Czyta arkusz i zwraca oczyszczoną tabelę:
    data_pozyskania, zestawienie_zrodel_ciepla, liczba_budynkow, udzial_procentowy
    """
    raw = pd.read_excel(
        io.BytesIO(excel_bytes),
        sheet_name=0,
        header=None,
        engine="openpyxl",
    )

    # usunięcie pustych wierszy i kolumn
    raw = raw.dropna(axis=0, how="all").dropna(axis=1, how="all").reset_index(drop=True)

    source_date = extract_source_date(raw)
    if source_date is None:
        source_date = pd.Timestamp.today().date().isoformat()

    # szukamy wiersza nagłówków
    header_mask = raw.apply(
        lambda row: (
            row.astype(str).str.contains(
                "Zestawienie źródeł ciepła", case=False, na=False
            ).any()
            and row.astype(str).str.contains(
                "Liczba", case=False, na=False
            ).any()
            and row.astype(str).str.contains(
                "Udział", case=False, na=False
            ).any()
        ),
        axis=1,
    )

    if not header_mask.any():
        raise ValueError("Nie znaleziono wiersza nagłówków w pliku XLSX.")

    header_idx = header_mask.idxmax()
    header_row = raw.loc[header_idx]

    def find_col(pattern: str) -> int:
        matches = header_row[
            header_row.astype(str).str.contains(pattern, case=False, na=False)
        ]
        if matches.empty:
            raise ValueError(f"Nie znaleziono kolumny pasującej do wzorca: {pattern}")
        return matches.index[0]

    source_col = find_col(r"Zestawienie źródeł ciepła")
    count_col = find_col(r"Liczba")
    share_col = find_col(r"Udział")

    data = raw.loc[header_idx + 1 :, [source_col, count_col, share_col]].copy()
    data.columns = [
        "zestawienie_zrodel_ciepla",
        "liczba_budynkow",
        "udzial_procentowy",
    ]

    # czyszczenie nazw
    data["zestawienie_zrodel_ciepla"] = (
        data["zestawienie_zrodel_ciepla"].astype("string").str.strip()
    )

    data = data[data["zestawienie_zrodel_ciepla"].notna()]
    data = data[
        ~data["zestawienie_zrodel_ciepla"].str.contains(
            "Dane pozyskane", case=False, na=False
        )
    ]

    # wywalamy puste/techniczne wiersze
    data = data[data["zestawienie_zrodel_ciepla"] != ""]
    data = data[data["zestawienie_zrodel_ciepla"] != "<NA>"]

    # liczby
    data["liczba_budynkow"] = (
        data["liczba_budynkow"]
        .astype("string")
        .str.replace("\xa0", "", regex=False)
        .str.replace(" ", "", regex=False)
        .str.strip()
    )
    data["liczba_budynkow"] = pd.to_numeric(data["liczba_budynkow"], errors="coerce").astype("Int64")

    # udział procentowy
    data["udzial_procentowy"] = (
        data["udzial_procentowy"]
        .astype("string")
        .str.replace("%", "", regex=False)
        .str.replace(",", ".", regex=False)
        .str.strip()
    )
    data["udzial_procentowy"] = pd.to_numeric(data["udzial_procentowy"], errors="coerce")

    # zostawiamy tylko sensowne rekordy
    data = data[
        data["zestawienie_zrodel_ciepla"].notna()
        & (
            data["liczba_budynkow"].notna()
            | data["udzial_procentowy"].notna()
        )
    ]

    # dodaj datę snapshotu
    data.insert(0, "data_pozyskania", source_date)

    # usuń duplikaty w obrębie nowego snapshotu
    data = data.drop_duplicates(
        subset=["data_pozyskania", "zestawienie_zrodel_ciepla"],
        keep="last",
    )

    return data.reset_index(drop=True)


def append_snapshot_to_csv(new_df: pd.DataFrame, csv_path: Path) -> pd.DataFrame:
    """
    Dopisuje nowy snapshot do CSV i pilnuje braku duplikatów:
    ten sam dzień + to samo zestawienie źródeł ciepła.
    """
    if csv_path.exists():
        old_df = pd.read_csv(csv_path, encoding="utf-8-sig")
    else:
        old_df = pd.DataFrame(columns=new_df.columns)

    old_df = old_df.reindex(columns=new_df.columns)

    combined = pd.concat([old_df, new_df], ignore_index=True)

    dup_mask = combined.duplicated(
        subset=["data_pozyskania", "zestawienie_zrodel_ciepla"],
        keep="last",
    )
    duplicates_count = int(dup_mask.sum())

    if duplicates_count > 0:
        print(f"Usuwam {duplicates_count} zduplikowanych wierszy.")

    combined = combined.drop_duplicates(
        subset=["data_pozyskania", "zestawienie_zrodel_ciepla"],
        keep="last",
    )

    combined["data_pozyskania"] = pd.to_datetime(combined["data_pozyskania"], errors="coerce")
    combined = combined.sort_values(
        by=["data_pozyskania", "zestawienie_zrodel_ciepla"],
        ascending=[True, True],
    ).reset_index(drop=True)
    combined["data_pozyskania"] = combined["data_pozyskania"].dt.strftime("%Y-%m-%d")

    combined.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return combined


def main() -> None:
    excel_bytes = download_file(REPORT_XLSX_URL)

    # zapis surowego pliku xlsx
    RAW_XLSX_PATH.write_bytes(excel_bytes)

    new_df = parse_zone_excel(excel_bytes)
    full_df = append_snapshot_to_csv(new_df, OUT_CSV_PATH)

    print(f"Zapisano XLSX: {RAW_XLSX_PATH.resolve()}")
    print(f"Zapisano / zaktualizowano CSV: {OUT_CSV_PATH.resolve()}")
    print()
    print("Nowy snapshot:")
    print(new_df)
    print()
    print("Cały plik:")
    print(full_df)


if __name__ == "__main__":
    main()
