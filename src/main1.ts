import "./style.css";
import * as d3 from "d3";

import { barChart } from "./part2(2)";
import { Int32, Table, Utf8 } from "apache-arrow";
import { db } from "./duckdb";
import parquet from "./airquality.parquet?url";

const app = document.querySelector("#app")!;

const chart = barChart();

async function update(Station: string) {
  const data: Table<{ "Main pollutant": Utf8; cnt: Int32 }> = await conn.query(`
  SELECT MP, count(*)::INT as cnt
  FROM airquality.parquet
  WHERE Station = '${ Station }'
  GROUP BY MP
  ORDER BY cnt DESC`);

  // Get the X and Y columns for the chart. Instead of using Parquet, DuckDB, and Arrow, we could also load data from CSV or JSON directly.
  const X = data.getChild("cnt")!.toArray();
  const Y = data
    .getChild("MP")!
    .toJSON()
    .map((d) => `${d}`);

  chart.update(X, Y);
}

const res = await fetch(parquet);
await db.registerFileBuffer(
  "airquality.parquet",
  new Uint8Array(await res.arrayBuffer())
);

// Query DuckDB for the cities.
const conn = await db.connect();

const Stations: Table<{ Station: Utf8 }> = await conn.query(`
SELECT DISTINCT Station
FROM airquality.parquet`);

// Create a select element for the cities.
const select = d3.select(app).append("select");
for (const Station of Stations) {
  select.append("option").text(Station.Station);
}

select.on("change", async () => {
  const Station = select.property("value");

  update(Station);
});

// Update the chart with the first city.
update("Avalon");

// Add the chart to the DOM.
app.appendChild(chart.element);