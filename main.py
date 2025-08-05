# -*- coding: utf-8 -*-

from fastapi import FastAPI, Request
from db import fn_get_connection
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import os

app = FastAPI()

#rutas csv sintéticos
SALES_CSV = "data/synthetic_sales_details.csv"
CASHFLOW_CSV = "data/synthetic_cash_flow.csv"
EBITDA_CSV = "data/synthetic_ebitda.csv"
RESERVAS_CSV = "data/synthetic_reservas.csv"
STOCK_CSV = "data/synthetic_stock.csv"


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
        "args": ["p_company_name", "p_venue_name", "p_year", "p_week_number"]
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
    }
}

def fallback_to_csv(fn_name, params):
    #lee los csv sintéticos
    if fn_name == "cash_flow_synthetic":
        df = pd.read_csv(CASHFLOW_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year"))
        ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}

    elif fn_name == "cash_flow_synthetic_by_week":
        df = pd.read_csv(CASHFLOW_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year")) &
            (df["p_week_number"] == params.get("p_week_number"))
        ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}

    elif fn_name == "cash_flow_synthetic_by_venue":
        df = pd.read_csv(CASHFLOW_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year")) &
            (df["p_venue_name"] == params.get("p_venue_name"))
        ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}

    elif fn_name == "cogs_synthetic":
        df = pd.read_csv(SALES_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year"))
        ]
        return {"result": "success", "data": filtered.to_dict(orient="records")}

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
    
    elif fn_name == "ebitda_synthetic":
        df = pd.read_csv(EBITDA_CSV)
        filtered = df[
            (df["p_company_name"] == params.get("p_company_name")) &
            (df["p_year"] == params.get("p_year"))
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

@app.post("/query")
async def run_query(request: Request):
  data = await request.json()
  fn_name = data.get("function")
  params: Dict = data.get("params", {})

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
    placeholders = ", ".join(["%s"] * len(arg_names))
    query = f"SELECT * FROM dwh.{fn_name}({placeholders});"
    
    args = [params.get(arg) for arg in arg_names]
    
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
