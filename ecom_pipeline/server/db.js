const { MongoClient } = require("mongodb");

const MONGO_URI = process.env.MONGO_URI;
const DB_NAME = "ecom_pipeline";

let db = null;
let client = null;

async function connect() {
  if (!MONGO_URI) {
    throw new Error("MONGO_URI missing");
  }

  client = new MongoClient(MONGO_URI, {
    serverSelectionTimeoutMS: 10000,
  });

  await client.connect();
  db = client.db(DB_NAME);

  console.log("[DB] Connected");
}

async function getSnapshot() {
  if (!db) {
    return {
      events: [],
      categories: [],
      products: [],
    };
  }

  const [events, categories, products] = await Promise.all([
    db.collection("raw_events").find({}).limit(50).toArray(),
    db.collection("category_stats").find({}).toArray(),
    db.collection("product_stats").find({}).limit(10).toArray(),
  ]);

  return { events, categories, products };
}

async function watchCollections(onChange) {
  if (!db) return;

  console.log("[DB] Watch skipped for now");
}

module.exports = {
  connect,
  getSnapshot,
  watchCollections,
};