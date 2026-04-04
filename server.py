from functools import lru_cache

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.middleware.cors import CORSMiddleware


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