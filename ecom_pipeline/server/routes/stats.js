const { Router } = require("express");
const { getSnapshot } = require("../db");

const router = Router();

/**
 * GET /api/stats
 * Full snapshot of all three collections — used for initial page load.
 */
router.get("/stats", async (req, res) => {
  try {
    const data = await getSnapshot();
    res.json({ success: true, data });
  } catch (err) {
    console.error("[API] /stats error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

/**
 * GET /api/events
 * Last 50 raw events only.
 */
router.get("/events", async (req, res) => {
  try {
    const { events } = await getSnapshot();
    res.json({ success: true, data: events });
  } catch (err) {
    console.error("[API] /events error:", err.message);
    res.status(500).json({ success: false, error: err.message });
  }
});

module.exports = router;
