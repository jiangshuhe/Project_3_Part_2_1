import "./style.css";
import * as d3 from "d3";

import { barChart } from "./part2(2)";
import { Int32, Table, Utf8 } from "apache-arrow";
import { db } from "./duckdb";
import parquet from "./AQ.parquet?url";

const app = document.querySelector("#app")!;

const chart = barChart();

async function update(City: string) {
  const data: Table<{ MP: Utf8; cnt: Int32 }> = await conn.query(`
  SELECT MP, count(*)::INT as cnt
  FROM airquality.parquet
  WHERE City = '${ City }'
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

const Cities: Table<{ City: Utf8 }> = await conn.query(`
SELECT DISTINCT City
FROM airquality.parquet`);

// Create a select element for the cities.
const select = d3.select(app).append("select");
for (const City of Cities) {
  select.append("option").text(City.City);
}

select.on("change", async () => {
  const City = select.property("value");

  update(City);
});

// Update the chart with the first city.
update("Avalon");

// Add the chart to the DOM.
app.appendChild(chart.element);