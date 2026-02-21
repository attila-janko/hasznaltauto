from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, Iterable, Optional


def connect_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS listings (
            ad_id INTEGER PRIMARY KEY,
            url TEXT,
            title TEXT,
            price_huf INTEGER,
            price_discount_huf INTEGER,
            currency TEXT,
            year INTEGER,
            year_month TEXT,
            mileage_km INTEGER,
            fuel TEXT,
            engine_cc INTEGER,
            power_kw INTEGER,
            power_hp INTEGER,
            transmission TEXT,
            drivetrain TEXT,
            body_type TEXT,
            color TEXT,
            seller_name TEXT,
            seller_type TEXT,
            location TEXT,
            description TEXT,
            equipment_json TEXT,
            images_json TEXT,
            attributes_json TEXT,
            raw_html TEXT,
            scraped_at TEXT
        );
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_listings_url ON listings(url);")
    conn.commit()


def listing_exists(conn: sqlite3.Connection, ad_id: int) -> bool:
    cur = conn.execute("SELECT 1 FROM listings WHERE ad_id = ? LIMIT 1;", (ad_id,))
    row = cur.fetchone()
    return row is not None


def upsert_listing(conn: sqlite3.Connection, data: Dict[str, Any]) -> None:
    defaults = {
        "ad_id": None,
        "url": None,
        "title": None,
        "price_huf": None,
        "price_discount_huf": None,
        "currency": None,
        "year": None,
        "year_month": None,
        "mileage_km": None,
        "fuel": None,
        "engine_cc": None,
        "power_kw": None,
        "power_hp": None,
        "transmission": None,
        "drivetrain": None,
        "body_type": None,
        "color": None,
        "seller_name": None,
        "seller_type": None,
        "location": None,
        "description": None,
        "equipment": [],
        "images": [],
        "attributes": {},
        "raw_html": None,
    }
    payload = {**defaults, **data}
    payload.setdefault("scraped_at", datetime.utcnow().isoformat(timespec="seconds") + "Z")
    payload["equipment_json"] = json.dumps(payload.get("equipment", []), ensure_ascii=True)
    payload["images_json"] = json.dumps(payload.get("images", []), ensure_ascii=True)
    payload["attributes_json"] = json.dumps(payload.get("attributes", {}), ensure_ascii=True)
    payload.pop("equipment", None)
    payload.pop("images", None)
    payload.pop("attributes", None)

    conn.execute(
        """
        INSERT INTO listings (
            ad_id, url, title, price_huf, price_discount_huf, currency,
            year, year_month, mileage_km, fuel, engine_cc, power_kw, power_hp,
            transmission, drivetrain, body_type, color, seller_name, seller_type,
            location, description, equipment_json, images_json, attributes_json,
            raw_html, scraped_at
        ) VALUES (
            :ad_id, :url, :title, :price_huf, :price_discount_huf, :currency,
            :year, :year_month, :mileage_km, :fuel, :engine_cc, :power_kw, :power_hp,
            :transmission, :drivetrain, :body_type, :color, :seller_name, :seller_type,
            :location, :description, :equipment_json, :images_json, :attributes_json,
            :raw_html, :scraped_at
        )
        ON CONFLICT(ad_id) DO UPDATE SET
            url = excluded.url,
            title = excluded.title,
            price_huf = excluded.price_huf,
            price_discount_huf = excluded.price_discount_huf,
            currency = excluded.currency,
            year = excluded.year,
            year_month = excluded.year_month,
            mileage_km = excluded.mileage_km,
            fuel = excluded.fuel,
            engine_cc = excluded.engine_cc,
            power_kw = excluded.power_kw,
            power_hp = excluded.power_hp,
            transmission = excluded.transmission,
            drivetrain = excluded.drivetrain,
            body_type = excluded.body_type,
            color = excluded.color,
            seller_name = excluded.seller_name,
            seller_type = excluded.seller_type,
            location = excluded.location,
            description = excluded.description,
            equipment_json = excluded.equipment_json,
            images_json = excluded.images_json,
            attributes_json = excluded.attributes_json,
            raw_html = excluded.raw_html,
            scraped_at = excluded.scraped_at;
        """,
        payload,
    )
    conn.commit()
