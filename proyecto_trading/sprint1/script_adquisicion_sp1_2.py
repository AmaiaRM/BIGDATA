import os
import logging
from datetime import datetime

import pandas as pd
from TradingviewData import TradingViewData, Interval


# Parámetros principales
SYMBOL = "ADAUSD"         
EXCHANGE = "BINANCE"
TIMEFRAME = "1d"
INTERVAL = Interval.daily

YEARS_HISTORY = 4
N_BARS = 1600
OUTPUT_ROOT = "./output" #dirctorio local para almacenar 

#incio de sesión
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

#crear directorios
def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def build_output_path(year: int, month: int):
    folder = os.path.join(
        OUTPUT_ROOT,
        "raw",
        "tradingview",
        "historical",
        f"symbol={SYMBOL}",
        f"timeframe={TIMEFRAME}",
        f"year={year}",
        f"month={month:02d}",
    )

    filename = f"{SYMBOL.lower()}_{TIMEFRAME}_{year}_{month:02d}.csv" #nombre de los archivos
    return folder, os.path.join(folder, filename)


def normalize_datetime_column(df: pd.DataFrame) -> pd.DataFrame:
    df = df.reset_index()
    #para que no de problemas con interpretación de fecha
    if "datetime" in df.columns:
        df = df.rename(columns={"datetime": "date"})
    elif "index" in df.columns:
        df = df.rename(columns={"index": "date"})
    elif "time" in df.columns:
        df = df.rename(columns={"time": "date"})
    elif "timestamp" in df.columns:
        df = df.rename(columns={"timestamp": "date"})
    else:
        # último recurso: buscar alguna columna que parezca fecha
        for c in df.columns:
            if "date" in c.lower() or "time" in c.lower():
                df = df.rename(columns={c: "date"})
                break

    if "date" not in df.columns:
        raise RuntimeError(f"No se ha podido identificar la columna temporal: {df.columns}")

    df["date"] = pd.to_datetime(df["date"])
    return df


def add_metadata(df: pd.DataFrame) -> pd.DataFrame:
    # Añadimos información común para trazabilidad
    if "source" not in df.columns:
        df.insert(0, "source", "tradingview")

    if "exchange" not in df.columns:
        df.insert(1, "exchange", EXCHANGE)

    # forzamos las columnas por si hay inconsistencias
    df["symbol"] = SYMBOL
    df["timeframe"] = TIMEFRAME
    df["ingestion_ts"] = datetime.utcnow().isoformat()

    return df


def main():
    tv = TradingViewData()

    df = tv.get_hist(
        symbol=SYMBOL,
        exchange=EXCHANGE,
        interval=INTERVAL,
        n_bars=N_BARS
    )

    if df is None or df.empty:
        raise RuntimeError("No se han obtenido datos de TradingView")

    df = normalize_datetime_column(df)

    # Nos quedamos solo con los últimos 4 años (01 de enero de 2022)
    start_year = datetime.now().year - YEARS_HISTORY
    cutoff = datetime(start_year, 1, 1)
    df = df[df["date"] >= cutoff]

    if df.empty:
        raise RuntimeError("No hay datos tras aplicar el filtro temporal")

    df = add_metadata(df)

    df = (
        df.sort_values("date")
          .drop_duplicates(subset=["symbol", "timeframe", "date"], keep="last")
    )

    total_rows = 0

    for (year, month), g in df.groupby([df["date"].dt.year, df["date"].dt.month]):
        folder, out_path = build_output_path(year, month)
        ensure_dir(folder)

        g.to_csv(out_path, index=False)
        total_rows += len(g)

    logging.info(f"Datos almacenados correctamente ({total_rows} registros)")


if __name__ == "__main__":
    main()
