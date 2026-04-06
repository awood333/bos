''' '''
from functools import lru_cache
import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse



app = FastAPI(title="BOS Local API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://127.0.0.1:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@lru_cache(maxsize=1)
def get_iu_merge_dependency():
    from container import get_dependency

    return get_dependency('i_u_merge')


def filter_iu_rows_by_wy_id(wy_id: str) -> list[dict]:
    obj = get_iu_merge_dependency()
    rows = obj.iu[obj.iu['WY_id'].astype(str) == str(wy_id)].copy()
    rows = rows.where(pd.notna(rows), None)
    return jsonable_encoder(rows.to_dict(orient='records'))


@app.get("/")
def read_root() -> dict[str, str]:
    return {"message": "BOS Local API is running"}


@app.get("/health")
def health_check() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/iu-merge/{wy_id}")
def get_iu_merge_rows(wy_id: str) -> dict[str, object]:
    rows = filter_iu_rows_by_wy_id(wy_id)
    if not rows:
        raise HTTPException(status_code=404, detail=f"No IU merge rows found for WY_id {wy_id}")

    return {
        "wy_id": wy_id,
        "count": len(rows),
        "rows": rows,
    }


# --- Net Revenue ---

_NET_REVENUE_PLOT_FOLDER = r"Q:\My Drive\COWS\data\plots\net_revenue_plots"


@lru_cache(maxsize=1)
def get_wet_dry_groups_dependency():
    from container import get_dependency
    return get_dependency('wet_dry_groups')


@app.get("/net-revenue/{wy_id}/monthly")
def get_net_revenue_monthly(wy_id: str) -> dict[str, object]:
    wdg = get_wet_dry_groups_dependency()
    df = wdg.net_revenue_wet_dry_df
    if df is None or df.empty:
        raise HTTPException(status_code=503, detail="Net revenue data not available")

    cow_df = df[df['WY_id'].astype(str) == str(wy_id)].copy()
    if cow_df.empty:
        raise HTTPException(status_code=404, detail=f"No net revenue data found for WY_id {wy_id}")

    cow_df['date'] = pd.to_datetime(cow_df['date'])
    cow_df['year'] = cow_df['date'].dt.year
    cow_df['month'] = cow_df['date'].dt.month

    monthly = (
        cow_df.groupby(['year', 'month'])
        .agg(revenue=('revenue', 'sum'), feedcost=('feedcost', 'sum'), net_revenue=('net_revenue', 'sum'))
        .reset_index()
        .round(0)
    )
    monthly = monthly.where(pd.notna(monthly), None)
    rows = jsonable_encoder(monthly.to_dict(orient='records'))
    return {"wy_id": wy_id, "count": len(rows), "rows": rows}


@app.get("/net-revenue/{wy_id}/plot")
def get_net_revenue_plot(wy_id: str):
    plot_path = os.path.join(_NET_REVENUE_PLOT_FOLDER, f"cow_{wy_id}_net_revenue.png")
    if not os.path.isfile(plot_path):
        raise HTTPException(status_code=404, detail=f"Plot not found for WY_id {wy_id}")
    return FileResponse(plot_path, media_type="image/png")