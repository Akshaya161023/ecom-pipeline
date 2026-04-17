const { MongoClient } = require("mongodb");

const MONGO_URI = "mongodb://localhost:27017/?directConnection=true";
const DB_NAME   = "ecom_pipeline";

let db;

/**
 * Connect to MongoDB and expose the db handle to the rest of the app.
 * Returns { client, db } so the caller can hold the client ref if needed.
 */
async function connect() {
  const client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 10_000,
  });
  await client.connect();
  db = client.db(DB_NAME);
  console.log("[DB] Connected to MongoDB —", MONGO_URI);
  return { client, db };
}

/**
 * Fetch a fresh snapshot of all three collections in parallel.
 * Used by both the REST endpoint and the WebSocket broadcaster.
 */
async function getSnapshot() {
  const [events, categories, products] = await Promise.all([
    db
      .collection("raw_events")
      .find({}, { projection: { _id: 0 } })
      .sort({ timestamp: -1 })
      .limit(50)
      .toArray(),
    db
      .collection("category_stats")
      .find({}, { projection: { _id: 0 } })
      .toArray(),
    db
      .collection("product_stats")
      .find({}, { projection: { _id: 0 } })
      .sort({ event_count: -1 })
      .limit(10)
      .toArray(),
  ]);
  return { events, categories, products };
}

/**
 * Watch raw_events, category_stats, and product_stats for any insert/update.
 * Calls onChange (debounced 500 ms) so a Spark batch-flush doesn't flood clients.
 */
async function watchCollections(onChange) {
  const names = ["raw_events", "category_stats", "product_stats"];
  let debounceTimer = null;

  for (const name of names) {
    const stream = db
      .collection(name)
      .watch([], { fullDocument: "updateLookup" });

    stream.on("change", () => {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(onChange, 500);
    });

    stream.on("error", (err) => {
      console.error(`[DB] Change stream error on '${name}':`, err.message);
    });
  }

  console.log("[DB] Change streams watching:", names.join(", "));
}

module.exports = { connect, getSnapshot, watchCollections };
