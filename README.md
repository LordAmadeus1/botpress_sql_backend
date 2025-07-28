# Botpress SQL Backend
This repository contains a lightweight backend API designed to connect Botpress chatbots with a PostgreSQL database. It acts as a bridge for retrieving Key Performance Indicator (KPI) data and other information from a Data Warehouse (DWH), allowing Botpress workflows to execute SQL queries dynamically.

# Features
*  REST API built with FastAPI.
*  Connects to PostgreSQL databases securely.
*  Executes stored functions or SQL queries with dynamic parameters.
*  Returns data in JSON format, making it easy to integrate with Botpress.
*  Designed to be deployed on Render or similar cloud platforms.

# API Endpoints
POST / Query
Executes a function or query in the Data Warehouse
Request Body Example:

``
{
  "function": "fn_weekly_avg_ticket_by_venue",
  "params": {
    "p_company_name": "PALLAPIZZA",
    "p_venue_name": null,
    "p_year": 2024,
    "p_week_number": 15,
    "p_month_number": null
  }
}
``

Response Example:
``
{
  "result": "success",
  "data": [
    {
      "venue_id": 1,
      "venue_name": "PAMPLONA",
      "avg_ticket_monday_current": 27.48,
      "avg_ticket_monday_last": null,
      "avg_ticket_tuesday_current": 55.97,
      "avg_ticket_tuesday_last": null
    }
  ]
} ``

#How to Deploy:

1. Clone repository:
   `` git clone https://github.com/LordAmadeus1/botpress_sql_backend.git
``
2. Install dependences:
   `` pip install -r requirements.txt ``
   
3. Set up enviroment variables:
   * DB_HOST
   * DB_PORT
   * DB_NAME
   * DB_USER
   * DB_PASSWORD
    
4. Run the API:
   `` uvicorn main:app --host 0.0.0.0 --port 8000
``
5. Deply the Render or another hosting service:

# Usage in Botpress
 * Call the /query endpoint directly from Botpress actions using fetch or axios
 * Save  the returned data into workflow variables
 * Format the resultl and show it to users in conversations

# Example Use Case
This backend is currently used as a temporary replacement for the server-dwh in staging environments. It enables Botpress bots to fetch KPI data such as sales, margins, cash flow, and projections directly from the Data Warehouse until the full MCP server integration is completed.
