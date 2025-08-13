import axios from "axios";

const DEFAULT_VENUES = ["PAMPLONA", "BILBAO", "BURGOS", "VITORIA", "ZARAGOZA", "SAN SEBASTIAN"];

async function run() {
  const backendUrl = process.env.BACKEND_URL;
  const today = new Date().toISOString().split("T")[0]; // YYYY-MM-DD

  if (!backendUrl) {
    console.error("❌ ERROR: BACKEND_URL no está definido");
    process.exit(1);
  }

  console.log(`📅 Generando reportes para el día ${today}...`);

  for (const venue of DEFAULT_VENUES) {
    try {
      const response = await axios.get(`${backendUrl}/daily_report`, {
        params: {
          url: backendUrl,
          venue_name: venue,
          date: today,
          lang: "es",
          tone: "funny"
        },
        headers: authToken ? { Authorization: `Bearer ${authToken}` } : {}
      });

      if (response.status !== 200 || !response.data) {
        console.warn(`⚠️ Error al obtener datos para ${venue}:`, response.statusText);
        continue;
      }

      const reportData = {
        date: today,
        venue,
        ...response.data.kpi_data,
        ...response.data.synthetic_data
      };

      // Guardar en backend como CSV
      await axios.post(`${backendUrl}/save_report_csv`, reportData, {
        headers:{}
      });

      console.log(`✅ Reporte guardado para ${venue}`);
    } catch (err) {
      console.error(`❌ Error procesando ${venue}:`, err.message);
    }
  }

  console.log("🎉 Todos los reportes procesados.");
}

run();