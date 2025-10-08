// UFC Database API - Node.js with Express and SQLite
// Install dependencies: npm install express better-sqlite3 cors

const express = require('express');
const Database = require('better-sqlite3');
const cors = require('cors');

const app = express();
const db = new Database('ufc_data.db', { readonly: true }); // readonly for safety

// Middleware
app.use(cors());
app.use(express.json());

// ============== FIGHTER ENDPOINTS ==============

// GET /api/fighters - Get all fighters
app.get('/api/fighters', (req, res) => {
  try {
    const fighters = db.prepare(`
      SELECT fighter_id, name, height, weight, reach, stance, dob,
             slpm, str_acc, sapm, str_def, td_avg, td_acc, td_def, sub_avg
      FROM fighters
      ORDER BY name
    `).all();
    
    res.json({ success: true, data: fighters });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/fighters/:id - Get specific fighter by ID
app.get('/api/fighters/:id', (req, res) => {
  try {
    const fighter = db.prepare(`
      SELECT * FROM fighters WHERE fighter_id = ?
    `).get(req.params.id);
    
    if (!fighter) {
      return res.status(404).json({ success: false, error: 'Fighter not found' });
    }
    
    res.json({ success: true, data: fighter });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/fighters/name/:name - Get fighter by name
app.get('/api/fighters/name/:name', (req, res) => {
  try {
    const fighter = db.prepare(`
      SELECT * FROM fighters WHERE name LIKE ?
    `).get(`%${req.params.name}%`);
    
    if (!fighter) {
      return res.status(404).json({ success: false, error: 'Fighter not found' });
    }
    
    res.json({ success: true, data: fighter });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/fighters/:id/history - Get fight history for a fighter
app.get('/api/fighters/:id/history', (req, res) => {
  try {
    const fights = db.prepare(`
      SELECT 
        e.event_name, e.event_date, e.event_location,
        f1.name as fighter, f2.name as opponent,
        fr.weight_class, fr.result, fr.method, fr.method_detail,
        fr.round, fr.time, fr.kd, fr.sig_str, fr.td, fr.sub,
        fr.fight_url
      FROM fight_results fr
      JOIN fighters f1 ON fr.fighter_id = f1.fighter_id
      LEFT JOIN fighters f2 ON fr.opponent_id = f2.fighter_id
      JOIN events e ON fr.event_id = e.event_id
      WHERE f1.fighter_id = ?
      ORDER BY e.event_date DESC
    `).all(req.params.id);
    
    res.json({ success: true, data: fights });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/fighters/:id/stats - Get fighter stats summary
app.get('/api/fighters/:id/stats', (req, res) => {
  try {
    const stats = db.prepare(`
      SELECT 
        f.name,
        COUNT(CASE WHEN fr.result = 'Win' THEN 1 END) as wins,
        COUNT(CASE WHEN fr.result = 'Loss' THEN 1 END) as losses,
        COUNT(CASE WHEN fr.result = 'Draw' THEN 1 END) as draws,
        COUNT(CASE WHEN fr.result = 'NC' THEN 1 END) as no_contests,
        COUNT(CASE WHEN fr.method = 'KO/TKO' AND fr.result = 'Win' THEN 1 END) as ko_wins,
        COUNT(CASE WHEN fr.method = 'SUB' AND fr.result = 'Win' THEN 1 END) as sub_wins,
        AVG(fr.sig_str) as avg_sig_strikes,
        AVG(fr.td) as avg_takedowns,
        SUM(fr.kd) as total_knockdowns
      FROM fighters f
      LEFT JOIN fight_results fr ON f.fighter_id = fr.fighter_id
      WHERE f.fighter_id = ?
      GROUP BY f.fighter_id, f.name
    `).get(req.params.id);
    
    if (!stats) {
      return res.status(404).json({ success: false, error: 'Fighter not found' });
    }
    
    res.json({ success: true, data: stats });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============== EVENT ENDPOINTS ==============

// GET /api/events - Get all events
app.get('/api/events', (req, res) => {
  try {
    const events = db.prepare(`
      SELECT event_id, event_name, event_date, event_location
      FROM events
      ORDER BY event_date DESC
    `).all();
    
    res.json({ success: true, data: events });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/events/:id - Get specific event
app.get('/api/events/:id', (req, res) => {
  try {
    const event = db.prepare(`
      SELECT * FROM events WHERE event_id = ?
    `).get(req.params.id);
    
    if (!event) {
      return res.status(404).json({ success: false, error: 'Event not found' });
    }
    
    res.json({ success: true, data: event });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/events/:id/fights - Get all fights for an event
app.get('/api/events/:id/fights', (req, res) => {
  try {
    const fights = db.prepare(`
      SELECT 
        f.name, fr.kd, fr.sig_str, fr.td, fr.sub,
        fr.result, fr.method, fr.method_detail, fr.round, fr.time,
        fr.weight_class, f2.name as opponent
      FROM fight_results fr
      JOIN fighters f ON fr.fighter_id = f.fighter_id
      LEFT JOIN fighters f2 ON fr.opponent_id = f2.fighter_id
      WHERE fr.event_id = ?
    `).all(req.params.id);
    
    res.json({ success: true, data: fights });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============== SEARCH & FILTER ENDPOINTS ==============

// GET /api/search/fighters?q=name - Search fighters by name
app.get('/api/search/fighters', (req, res) => {
  try {
    const query = req.query.q || '';
    const fighters = db.prepare(`
      SELECT fighter_id, name, height, weight, reach, stance
      FROM fighters
      WHERE name LIKE ?
      ORDER BY name
      LIMIT 20
    `).all(`%${query}%`);
    
    res.json({ success: true, data: fighters });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/fights/recent?limit=10 - Get recent fights
app.get('/api/fights/recent', (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 10;
    const fights = db.prepare(`
      SELECT 
        e.event_name, e.event_date,
        f1.name as fighter1, f2.name as fighter2,
        fr.weight_class, fr.result, fr.method,
        fr.round, fr.time
      FROM fight_results fr
      JOIN fighters f1 ON fr.fighter_id = f1.fighter_id
      LEFT JOIN fighters f2 ON fr.opponent_id = f2.fighter_id
      JOIN events e ON fr.event_id = e.event_id
      ORDER BY e.event_date DESC, fr.fight_id DESC
      LIMIT ?
    `).all(limit);
    
    res.json({ success: true, data: fights });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/fights/matchup/:fighter1/:fighter2 - Get head-to-head history
app.get('/api/fights/matchup/:fighter1/:fighter2', (req, res) => {
  try {
    const fights = db.prepare(`
      SELECT 
        e.event_name, e.event_date,
        f1.name as fighter1, f2.name as fighter2,
        fr.result as fighter1_result, fr.method, fr.round, fr.time,
        fr.kd as fighter1_kd, fr.sig_str as fighter1_strikes,
        fr2.kd as fighter2_kd, fr2.sig_str as fighter2_strikes
      FROM fight_results fr
      JOIN fighters f1 ON fr.fighter_id = f1.fighter_id
      JOIN fighters f2 ON fr.opponent_id = f2.fighter_id
      JOIN events e ON fr.event_id = e.event_id
      LEFT JOIN fight_results fr2 ON fr2.event_id = fr.event_id 
        AND fr2.fighter_id = f2.fighter_id 
        AND fr2.opponent_id = f1.fighter_id
      WHERE (f1.fighter_id = ? AND f2.fighter_id = ?)
      ORDER BY e.event_date DESC
    `).all(req.params.fighter1, req.params.fighter2);
    
    res.json({ success: true, data: fights });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// ============== STATISTICS ENDPOINTS ==============

// GET /api/stats/top-strikers?limit=10
app.get('/api/stats/top-strikers', (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 10;
    const fighters = db.prepare(`
      SELECT name, slpm, str_acc, sapm, str_def
      FROM fighters
      WHERE slpm IS NOT NULL
      ORDER BY slpm DESC
      LIMIT ?
    `).all(limit);
    
    res.json({ success: true, data: fighters });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// GET /api/stats/top-grapplers?limit=10
app.get('/api/stats/top-grapplers', (req, res) => {
  try {
    const limit = parseInt(req.query.limit) || 10;
    const fighters = db.prepare(`
      SELECT name, td_avg, td_acc, td_def, sub_avg
      FROM fighters
      WHERE td_avg IS NOT NULL
      ORDER BY td_avg DESC
      LIMIT ?
    `).all(limit);
    
    res.json({ success: true, data: fighters });
  } catch (error) {
    res.status(500).json({ success: false, error: error.message });
  }
});

// Health check endpoint
app.get('/api/health', (req, res) => {
  res.json({ success: true, status: 'API is running', timestamp: new Date() });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({ success: false, error: 'Endpoint not found' });
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`UFC API server running on port ${PORT}`);
  console.log(`API endpoints available at http://localhost:${PORT}/api/`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  db.close();
  process.exit(0);
});