# -*- coding: utf-8 -*-

from fastapi import FastAPI, Query, Body, Request
from db import fn_get_connection
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import requests
import csv
import os
from typing import Optional
from pathlib import Path
import asyncio
from datetime import datetime, date, timedelta
import calendar
from ingest.daily import run_daily_weather_ingest
from clients.visual_crossing import fetch_weather_for_city, upsert_daily_weather_csv_async

app = FastAPI()

#rutas csv sintéticos
SALES_CSV = "data/synthetic_sales_details.csv"
CASHFLOW_CSV = "data/synthetic_cash_flow.csv"
EBITDA_CSV = "data/synthetic_ebitda.csv"
RESERVAS_CSV = "data/synthetic_reservas.csv"
STOCK_CSV = "data/synthetic_stock.csv"

EVENTS_CSV = "data/daily_events.csv"
MOTIVATION_CSV = "data/motivational_phrases.csv"

DATA_DIR = Path("/weather")
WEATHER_CSV = str(DATA_DIR / "daily_weather.csv")

REPORTS_CSV = str(DATA_DIR / "daily_reports.csv")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  #["https://tu-botpress-url.com"] para restringirlo
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

kpi_function_map = {
    "fn_weekly_avg_ticket_by_venue": {
        "args": ["p_company_name", "p_week_number", "p_year"]
    },
    "fn_estimated_profit_by_company_and_period": {
        "args": ["p_company_name", "p_year", "p_week_number", "p_month_number"]
    },
    "fn_estimated_profit_by_venue_and_period": {
        "args": ["p_company_name", "p_venue_name", "p_year", "p_week_number", "p_month_number"]
    },
    "fn_estimated_profit_by_venues_and_week": {
        "args": ["p_company_name", "p_year", "p_week_number"]
    },
    "fn_personnel_expense_ratio": {
        "args": ["p_company_name", "p_year", "p_venue_name","p_week_number", "p_month_number"]
    },
    "fn_total_income_by_period": {
        "args": ["p_company_name", "p_year", "p_week_number", "p_month_number"]
    },
    "fn_week_total_attendees": {
        "args": ["p_company_name", "p_week_number", "p_year"]
    },
    "fn_weekly_attendance_by_venue": {
        "args": ["p_company_name", "p_week_number", "p_year"]
    },
    "fn_weekly_avg_income_per_attendee": {
        "args": ["p_company_name", "p_week_number", "p_year"]
    },
    "fn_weekly_sales_comparison_by_section": {
        "args": ["p_company_name", "p_week_number", "p_year"]
    },
    "fn_weekly_venues_income": {
        "args": ["p_company_name", "p_week_number", "p_year", "p_month_number"]
    },
    "get_debit_variation_by_company_and_period": {
        "args": ["p_company_name", "p_week_number", "p_year", "p_month_number"]
    },
    "get_debit_variation_by_venue_and_period": {
        "args": ["p_company_name", "p_venue_name", "p_year","p_week_number", "p_month_number"]
    },
    "get_venue_income_by_period": {
        "args": ["p_company_name", "p_venue_name" , "p_year", "p_week_number", "p_month_number"]
    },
    "fn_personnel_expense_ratio2": {
        "args": ["p_company_name", "p_venue_name", "p_year", "p_week_number", "p_month_number"]
    },
    "fn_weekly_total_income_no_digital": {
        "args": ["p_company_name", "p_week_number", "p_year"]
    },
    "fn_weekly_venues_income_no_digital": {
        "args": ["p_company_name", "p_week_number", "p_year"]
    },
    ".get_departmental_expenses": {
        "args": ["p_company_name", "p_year", "p_month_number"]
    }
    }

@app.get("/weather")
def get_weather(city: str, date_str: str):
    if not os.path.exists(WEATHER_CSV):
        return {"result": "error", "message": "No weather data"}
    df = pd.read_csv(WEATHER_CSV)
    filtered = df[df["city"].str.contains(city, case=False, na=False) & (df["date"].astype(str) == str(date_str).strip())]
    return {"result": "success", "data": filtered.to_dict(orient="records")}

@app.post("/ingest/daily-weather")
async def ingest_daily_weather(payload: dict = Body(default={})):
    venues = payload.get("venues")  # opcional; si no, usa las por defecto
    start_date = payload.get("start_date")
    end_date   = payload.get("end_date") 
    
    res = await run_daily_weather_ingest(venues=venues, start_date=start_date, end_date=end_date)
    return res

async def handle_weather_forecast(params: dict):

    city = str(params.get("p_venue_name") or params.get("p_city") or "").strip()

    if not city:
        return {
            "error": "Faltan parámetros requeridos: p_venue_name/p_city"
        }

    start_date_str = params.get("p_start_date")
    end_date_str = params.get("p_end_date")

    if not start_date_str:
        today_iso      = date.today().isoformat()
        start_date_str = today_iso
        end_date_str   = today_iso
    elif not end_date_str:
        # Si solo falta end_date, lo igualamos a start_date
        end_date_str   = start_date_str

    start_dt = pd.to_datetime(start_date_str).date()
    end_dt   = pd.to_datetime(end_date_str).date()

    start_iso = start_dt.isoformat()
    end_iso   = end_dt.isoformat()

    rows = await fetch_weather_for_city(city, start_iso, end_iso)

    for row in rows:
        await upsert_daily_weather_csv_async(row)
        
    return {"result": "success", "data": rows}

def fallback_to_csv(fn_name, params):
    #lee los csv sintéticos
    if fn_name == "cash_flow_synthetic_by_week":
        df = pd.read_csv(CASHFLOW_CSV)
        filtered = df[
            (df["p_year"] == params.get("p_year")) &
            (df["p_week_number"] == params.get("p_week_number"))
        ]
        cols_income = [c for c in df.columns if c.endswith("_income_predicted")]
        result_df = filtered[["p_venue_name", "p_year", "p_week_number"] + cols_income]
        
        return {"result": "success", "data": result_df.to_dict(orient="records")}

    elif fn_name == "cash_flow_synthetic_by_venue":
        df = pd.read_csv(CASHFLOW_CSV)
        filtered = df[
            (df["p_venue_name"] == params.get("p_venue_name")) &
            (df["p_year"] == params.get("p_year")) &
            (df["p_week_number"] == params.get("p_week_number"))
        ]
        cols_income = [c for c in df.columns if c.endswith("_income_predicted")]
        result_df = filtered[["p_venue_name", "p_year", "p_week_number"] + cols_income]
        return {"result": "success", "data": result_df.to_dict(orient="records")}

    elif fn_name == "cogs_synthetic_by_venue":
        df = pd.read_csv(SALES_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year")) &
            (df["p_venue_name"] == params.get("p_venue_name"))
        ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}

    elif fn_name == "cogs_synthetic_by_week":
        df = pd.read_csv(SALES_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year")) &
            (df["p_week_number"] == params.get("p_week_number"))
        ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}


    elif fn_name == "ebitda_synthetic_by_month":
        df = pd.read_csv(EBITDA_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year")) &
            (df["p_month_number"] == params.get("p_month_number"))
        ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}

    elif fn_name == "ebitda_synthetic_by_venue":
        df = pd.read_csv(EBITDA_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year")) &
            (df["p_venue_name"] == params.get("p_venue_name"))
        ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}

    elif fn_name == "reservas_synthetic_by_week":
        df = pd.read_csv(RESERVAS_CSV)
        # Filtro base por compañía (no filtramos por año todavía porque podría cruzar)
        filtered_base = df[(df["p_company_name"] == params.get("p_company_name"))]
        year = params.get("p_year")
        week_start = params.get("p_week_number")
        week_end = params.get("p_week_number_end")
    
        if week_start is not None and week_end is None:
            # Solo una semana, mismo año
            filtered = filtered_base[
                (filtered_base["p_year"] == year) &
                (filtered_base["p_week_number"] == week_start)
            ]
        elif week_start is not None and week_end is not None:
            if week_end >= week_start:
                # Rango dentro del mismo año
                filtered = filtered_base[
                    (filtered_base["p_year"] == year) &
                    (filtered_base["p_week_number"] >= week_start) &
                    (filtered_base["p_week_number"] <= week_end)
                ]
            else:
                # Rango cruza de año: [week_start..última del año] ∪ [1..week_end del año siguiente]
                filtered_current_year = filtered_base[
                    (filtered_base["p_year"] == year) &
                    (filtered_base["p_week_number"] >= week_start)
                ]
                filtered_next_year = filtered_base[
                    (filtered_base["p_year"] == year + 1) &
                    (filtered_base["p_week_number"] <= week_end)
                ]
                filtered = pd.concat([filtered_current_year, filtered_next_year], ignore_index=True)
        else:
            # Sin semanas provistas → devuelve solo el año solicitado (o vacío si prefieres)
            filtered = filtered_base[(filtered_base["p_year"] == year)]
    
        return {"result": "success", "data": filtered.to_dict(orient="records")}
        
    elif fn_name == "reservas_synthetic_by_venue":
        df = pd.read_csv(RESERVAS_CSV)
        # Filtro base por compañía y venue (no filtramos por año todavía porque podría cruzar)
        filtered_base = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_venue_name"] == params.get("p_venue_name"))
        ]
    
        year = params.get("p_year")
        week_start = params.get("p_week_number")
        week_end = params.get("p_week_number_end")
    
        if week_start is not None and week_end is None:
            # Solo una semana, mismo año
            filtered = filtered_base[
                (filtered_base["p_year"] == year) &
                (filtered_base["p_week_number"] == week_start)
            ]
    
        elif week_start is not None and week_end is not None:
            if week_end >= week_start:
                # Rango dentro del mismo año
                filtered = filtered_base[
                    (filtered_base["p_year"] == year) &
                    (filtered_base["p_week_number"] >= week_start) &
                    (filtered_base["p_week_number"] <= week_end)
                ]
            else:
                # Rango cruza de año: [week_start..última del año] ∪ [1..week_end del año siguiente]
                filtered_current_year = filtered_base[
                    (filtered_base["p_year"] == year) &
                    (filtered_base["p_week_number"] >= week_start)
                ]
                filtered_next_year = filtered_base[
                    (filtered_base["p_year"] == year + 1) &
                    (filtered_base["p_week_number"] <= week_end)
                ]
                filtered = pd.concat([filtered_current_year, filtered_next_year], ignore_index=True)
        else:
            # Sin semanas provistas → devuelve solo el año solicitado (o vacío si prefieres)
            filtered = filtered_base[(filtered_base["p_year"] == year)]
    
        return {"result": "success", "data": filtered.to_dict(orient="records")}


    elif fn_name == "stock_synthetic_by_week":
        df = pd.read_csv(STOCK_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year"))
        ]
        if params.get("p_week_number") is not None:
            filtered = filtered[filtered["p_week_number"] == params.get("p_week_number")]
        elif params.get("p_week_start") is not None and params.get("p_week_end") is not None:
            filtered = filtered[
                (filtered["p_week_number"] >= params.get("p_week_start")) &
                (filtered["p_week_number"] <= params.get("p_week_end"))
            ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}

    elif fn_name == "stock_synthetic_by_venue":
        df = pd.read_csv(STOCK_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_venue_name"] == params.get("p_venue_name")) &
            (df["p_year"] == params.get("p_year"))
        ]
        if params.get("p_week_number") is not None:
            filtered = filtered[filtered["p_week_number"] == params.get("p_week_number")]
        elif params.get("p_week_start") is not None and params.get("p_week_end") is not None:
            filtered = filtered[
                (filtered["p_week_number"] >= params.get("p_week_start")) &
                (filtered["p_week_number"] <= params.get("p_week_end"))
            ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}
        
    # Si no encontramos el KPI ni en CSV
    return {"result": "error", "message": f"No data found for {fn_name}"}
    

@app.get("/")
def read_root():
    return {"message": "Backend connected"}

@app.middleware("http")
async def log_requests(request: Request, call_next):
    print(f"📥 Incoming request: {request.method} {request.url}")
    response = await call_next(request)
    return response

@app.post("/query")
async def run_query(request: Request):
  data = await request.json()
  print("data: ", data)
  fn_name = data.get("function")
  params: Dict = data.get("params", {})

  if fn_name == "weather_forecast":
      return await handle_weather_forecast(params)

  try:
    print("🔵 Parámetros recibidos:", params)
    print("🔵 Función solicitada:", fn_name)
      
    conn = fn_get_connection()
    print("Conectado a la base de datos")
    cur = conn.cursor()
    cur.execute("SET search_path TO dwh, public;")
    
    fn_info = kpi_function_map.get(fn_name)
    if not fn_info:
        print(f"[WARM] Función {fn_name} no está en el mapa, activando fallback")
        return fallback_to_csv(fn_name, params)
        
    arg_names = fn_info["args"]
    args = [params.get(arg) for arg in arg_names]
    
    placeholders = ", ".join(["%s"] * len(args))
    query = f"SELECT * FROM dwh.{fn_name}({placeholders});"
    
    
    print("🟢 Ejecutando:", query)
    print("📦 Con args:", args)
    
    cur.execute(query, args)
    
    result = cur.fetchall()
    
    cur.close()
    conn.close()
    
    print("bien, consulta bien")

    if result:
        return {"result": "success", "data": result}

    print(f"[WARN] No hay datos en DWH para {fn_name}, activando fallback CSV")
      
    return fallback_to_csv(fn_name, params)
    
  except Exception as e:
    print("error al ejecutar", e)
    return {"status": "error", "message": str(e)}

@app.get("/events")
def get_events(
    date_str: str = Query(..., description="Fecha YYYY-MM-DD"),
    city: str | None = Query(None, description="Ciudad (opcional, si se omite devuelve nacionales)")
):
    # Validación de fecha
    try:
        _ = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return {"result": "error", "message": "date_str debe tener formato YYYY-MM-DD"}

    if not  os.path.exists(EVENTS_CSV):
        return {"result": "error", "message": "No events data"}

    # Leemos como texto para evitar NaNs (nacionales guardadas como vacío)
    df = pd.read_csv(EVENTS_CSV, dtype=str).fillna("")
    df = df[df["date"] == date_str]

    if city:
        city_norm = city.strip().upper()
        # city en CSV puede venir en mayúsculas o vacío para nacionales
        df = df[df["city"].str.upper() == city_norm]
    else:
        # Nacionales: city vacía
        df = df[df["city"] == ""]

    data = df.to_dict(orient="records")
    return {"result": "success", "data": data}

@app.get("/motivation")
def get_motivation(
    date_str: str = Query(..., description="Fecha YYYY-MM-DD para elegir frase del día"),
    lang: str = Query("es", description="Idioma (por defecto 'es')"),
    tone: str = Query("funny", description="Tono (por defecto 'funny')")
):
    # Validación de fecha
    try:
        target_date = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        return {"result": "error", "message": "date_str debe tener formato YYYY-MM-DD"}

    if not os.path.exists(MOTIVATION_CSV):
        return {"result": "error", "message": "No motivation data"}

    df = pd.read_csv(MOTIVATION_CSV, dtype=str).fillna("")

    # Filtro por idioma/tono
    subset = df[(df["lang"].str.lower() == lang.lower()) & (df["tone"].str.lower() == tone.lower())]

    if subset.empty:
        # Fallback: ignora filtros si no hay coincidencias
        subset = df.copy()

    if subset.empty:
        return {"result": "error", "message": "No hay frases disponibles"}

    # Selección determinista de "frase del día"
    idx = hash((date_str, lang.lower(), tone.lower())) % len(subset)
    row = subset.iloc[idx].to_dict()

    return {"result": "success", "data": [row]}

def get_weekday_label(dt:date):
  return calendar.day_abbr[dt.weekday()].lower()

def get_weekday_number(dt:date):
  return dt.weekday()

async def ingest_one_day(city: str, date_str: str):
    rows = await fetch_weather_for_city(city, date_str)
    if not rows:
        raise RuntimeError(f"No se obtuvieron datos para {city} {date_str}")
    for row in rows:
        await upsert_daily_weather_csv_async(row)


@app.get("/daily_report")
async def get_daily_report(url: str, venue_name :str, date : datetime,lang:str="es", tone:str ="funny"):

  company = "PALLAPIZZA"
  target_date = pd.to_datetime(date).date()
  year = target_date.year
  week_number = target_date.isocalendar().week
  weekday_label = get_weekday_label(target_date)  # 'mon', 'tue', etc.
  weekday_label_full = target_date.strftime("%A").lower()
  weekday_number = get_weekday_number(target_date)
  date_str = target_date.isoformat()

  #kpi_data
  # we need last_year_{weekday} as objective
  response = requests.post(f"{url}/query",
        json={
            "function": "fn_weekly_venues_income",
            "params": {
                "p_company_name": company,
                "p_year": year,
                "p_week_number": week_number
            }
        })

  if response.status_code != 200:
        return {"error": "API call failed", "details": response.text}

  try:
    kpi_data = response.json()["data"]
  except json.JSONDecodeError as e:
    return {"error": "Invalid JSON response", "details": str(e)}

  #search by venue
  venue_data = next((item for item in kpi_data if item["venue_name"].upper() == venue_name.upper()), None)
  if venue_data is None:
    return {"error": "Venue not found"}

  #previous year as objective
  target_income = venue_data.get(f"last_year_{weekday_label_full}", 0)

  response_att = requests.post(f"{url}/query", json={
        "function": "fn_weekly_attendance_by_venue",
        "params": {
            "p_company_name": company,
            "p_year": year,
            "p_week_number": week_number
        }
    })
  
  attendance_data = response_att.json().get("data", [])
  attendance_row = next((row for row in attendance_data if row["venue_name"].upper() == venue_name.upper()), None) 

  if attendance_row:
        attendance_last = attendance_row.get(f"{weekday_label}_prev", 0)
        current_attendance = int(attendance_last * 1.1)
        if attendance_last > 0:
            attendance_variation = ((current_attendance - attendance_last) / attendance_last) * 100
        else:
            attendance_variation = 0.0
  else:
      attendance_last = 0
      current_attendance = 0
      attendance_variation = 0

  reservas_df = pd.read_csv(RESERVAS_CSV)
  reservas_filtered = reservas_df[
      (reservas_df["p_company_name"] == company) &
      (reservas_df["p_venue_name"] == venue_name) &
      (reservas_df["p_year"] == year) &
      (reservas_df["p_week_number"] == week_number) &
      (reservas_df["weekday"] == weekday_number)
  ]
  if not reservas_filtered.empty:
      num_reservas = int(reservas_filtered.iloc[0]["reservations"])
  else: num_reservas = 0

  # 2. STOCK
  stock_df = pd.read_csv(STOCK_CSV)
  stock_filtered = stock_df[
      (stock_df["p_company_name"] == company) &
      (stock_df["p_venue_name"] == venue_name) &
      (stock_df["p_year"] == year) &
      (stock_df["p_week_number"] == week_number)
  ].copy()

  stock_filtered["ratio"] = stock_filtered["stock"] / stock_filtered["capacity"]
  productos_bajo_stock = stock_filtered[stock_filtered["ratio"] < 0.3]["product_name"].tolist()
  productos_medio_stock = stock_filtered[
      (stock_filtered["ratio"] >= 0.3) & (stock_filtered["ratio"] < 0.6)
  ]["product_name"].tolist()

  #motivation
  if not os.path.exists(MOTIVATION_CSV):
      phrase= "¡Ánimo! Hoy es un gran día para intentarlo."

  else:
    df = pd.read_csv(MOTIVATION_CSV, dtype=str).fillna("")

    filtered = df[
        (df["lang"].str.lower() == lang.lower()) &
        (df["tone"].str.lower() == tone.lower())
    ]

    if filtered.empty:
        filtered = df.copy()
    phrase= filtered.iloc[hash((str(target_date), lang.lower(), tone.lower())) % len(filtered)]["text"]

  #events
  if not os.path.exists(EVENTS_CSV):
        events, hay_futbol= [], False

  else:
    df = pd.read_csv(EVENTS_CSV, dtype=str).fillna("")
    df_today = df[(df["date"] == target_date.isoformat()) & (df["city"].str.upper() == venue_name.upper())]
    events = df_today["title"].tolist()
    hay_futbol = any(df_today["has_football"].astype(str) == "1")

  #weather
  def generar_frase_clima(clima):
    if clima:
        if "rain" in clima or "lluvia" in clima:
            return "Parece que lloverá ☔, tenlo en cuenta para las reservas."
        elif "sun" in clima or "despejado" in clima:
            return "¡Día soleado! La terraza seguro que se llena. ☀️"
        elif "cloud" in clima or "nublado" in clima:
            return "Día nublado, perfecto para comer algo caliente."
        else:
            return f"El clima de hoy es {clima}."
    return "No tengo información del clima para hoy."
      
  weather_url = f"{url}/weather"
  df_weather = pd.read_csv(WEATHER_CSV)
  df_weather["date"] = pd.to_datetime(df_weather["date"]).dt.date

  target_date = pd.to_datetime(date).date()
  date_str = target_date.isoformat()

  mask = (df_weather["city"].str.contains(venue_name, case=False, na=False) &
    (df_weather["date"] == target_date))

  weather_row = df_weather[mask]

  if not weather_row.empty:
    row = weather_row.iloc[0]
    temperatura = weather_row.iloc[0]["temp"]
    clima = str(weather_row.iloc[0]["conditions"]).lower()
    icono = weather_row.iloc[0]["icon"]
    frase_clima = generar_frase_clima(clima)

  else:
    try:
        await ingest_one_day(venue_name, date_str)
        df = pd.read_csv(WEATHER_CSV)
        df["date"] = pd.to_datetime(df["date"]).dt.date
        weather_row = df[df["city"].str.contains(venue_name, case=False, na=False) &
                (df["date"] == target_date)]

        if not weather_row.empty:
            row = weather_row.iloc[0]
            temperatura = row["temp"]
            clima = str(row["conditions"]).lower()
            icono = row["icon"]
            frase_clima = generar_frase_clima(clima)
        else:
            temperatura = clima = icono = None
            frase_clima = "No tengo información del clima para hoy."

    except Exception as e:
        temperatura = clima = icono = None
        frase_clima = f"No se pudo ingestar clima: {e}"

  #cash_flow  
  cashflow_df = pd.read_csv(CASHFLOW_CSV)
  daily_income_col = f"{weekday_label_full}_income_predicted"

  pred_row = cashflow_df[
    (cashflow_df["p_venue_name"].str.upper() == venue_name.upper()) &
    (cashflow_df["p_year"] == year) &
    (cashflow_df["p_week_number"] == week_number)
  ]

  if not pred_row.empty and daily_income_col in cashflow_df.columns:
    daily_income_predicted = float(pred_row.iloc[0][daily_income_col])
  else:
    daily_income_predicted = None


  if daily_income_predicted is not None and target_income > 0:
    daily_income_var = round(((daily_income_predicted - target_income) / target_income) * 100, 2)
  else:
    daily_income_var = None


  if "frase_clima" not in locals():
      frase_clima = "No tengo información detallada del clima."
  return {
      "result": "success",
      "kpi_data": {
          "objective": target_income,
          "prediction": daily_income_predicted,
          "prediction_var": daily_income_var,
          "attendance_last": attendance_last,
          "attendance_variation": attendance_variation,
          "num_reservas": num_reservas
      },
      "synthetic_data": {
          "productos_bajo_stock": productos_bajo_stock,
          "productos_medio_stock": productos_medio_stock,
          "fechas_importantes": events,
          "clima": clima,
          "temperatura": temperatura,
          "frase_clima": frase_clima,
          "frase_motivacional": phrase,
          "hay_futbol": hay_futbol
      }
  }


@app.post("/save_report_csv")
async def save_report_csv(request: Request):
    data = await request.json()

    # Si el archivo no existe, crea cabeceras
    file_exists = os.path.exists(REPORTS_CSV)

    with open(REPORTS_CSV, "a", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=data.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)

    return {"status": "success", "message": "Reporte guardado"}
