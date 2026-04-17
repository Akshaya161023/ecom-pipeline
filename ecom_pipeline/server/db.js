const { MongoClient } = require("mongodb");

// Read MongoDB Atlas connection string from environment variable
const MONGO_URI = process.env.MONGO_URI;

// Database name
const DB_NAME = "ecom_pipeline";

let db;
let client;

/**
 * Connect to MongoDB Atlas and expose db handle
 */
async function connect() {
  try {
    client = new MongoClient(MONGO_URI, {
      serverSelectionTimeoutMS: 10000,
    });

    await client.connect();

    db = client.db(DB_NAME);

    console.log("[DB] Connected successfully");

    return { client, db };
  } catch (err) {
    console.error("[DB] Connection failed:", err.message);
    throw err;
  }
}

/**
 * Get live dashboard snapshot
 */
async function getSnapshot() {
  if (!db) throw new Error("Database not connected");

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

  return {
    events,
    categories,
    products,
  };
}

/**
 * Watch collections and trigger updates
 */
async function watchCollections(onChange) {
  if (!db) throw new Error("Database not connected");

  const collections = [
    "raw_events",
    "category_stats",
    "product_stats",
  ];

  let debounceTimer = null;

  for (const name of collections) {
    const stream = db
      .collection(name)
      .watch([], { fullDocument: "updateLookup" });

    stream.on("change", () => {
      clearTimeout(debounceTimer);
      debounceTimer = setTimeout(onChange, 500);
    });

    stream.on("error", (err) => {
      console.error(`[DB] Change stream error in ${name}:`, err.message);
    });
  }

  console.log("[DB] Watching collections:", collections.join(", "));
}

/**
 * Optional graceful shutdown
 */
async function closeConnection() {
  if (client) {
    await client.close();
    console.log("[DB] Connection closed");
  }
}

module.exports = {
  connect,
  getSnapshot,
  watchCollections,
  closeConnection,
};