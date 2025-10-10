const express = require('express');
const Database = require('better-sqlite3');
const cors = require('cors');

const app = express();
const PORT = process.env.PORT || 3000;

// Middleware
app.use(cors());
app.use(express.json());

// Database connection
const db = new Database('./ufc_stats.db');
console.log('Connected to UFC stats database');

// =============================================
// EVENT ENDPOINTS
// =============================================

// GET all events
app.get('/api/events', (req, res) => {
  try {
    const stmt = db.prepare(`
      SELECT 
        event_id,
        event_name,
        event_date,
        location,
        created_at
      FROM events
      ORDER BY event_date DESC
    `);
    const events = stmt.all();
    
    res.json({
      success: true,
      count: events.length,
      data: events
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET single event by ID
app.get('/api/events/:id', (req, res) => {
  try {
    const stmt = db.prepare(`
      SELECT 
        event_id,
        event_name,
        event_date,
        location,
        created_at
      FROM events
      WHERE event_id = ?
    `);
    const event = stmt.get(req.params.id);
    
    if (!event) {
      return res.status(404).json({
        success: false,
        error: 'Event not found'
      });
    }
    
    res.json({
      success: true,
      data: event
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET all fights for a specific event
app.get('/api/events/:id/fights', (req, res) => {
  try {
    const stmt = db.prepare(`
      SELECT 
        f.fight_id,
        f.event_id,
        e.event_name,
        f1.fighter_id as fighter1_id,
        f1.name as fighter1_name,
        f1.nickname as fighter1_nickname,
        f2.fighter_id as fighter2_id,
        f2.name as fighter2_name,
        f2.nickname as fighter2_nickname,
        w.fighter_id as winner_id,
        w.name as winner_name,
        f.method,
        f.round,
        f.time,
        f.created_at
      FROM fights f
      JOIN events e ON f.event_id = e.event_id
      JOIN fighters f1 ON f.fighter1_id = f1.fighter_id
      JOIN fighters f2 ON f.fighter2_id = f2.fighter_id
      LEFT JOIN fighters w ON f.winner_id = w.fighter_id
      WHERE f.event_id = ?
    `);
    const fights = stmt.all(req.params.id);
    
    res.json({
      success: true,
      count: fights.length,
      data: fights
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// =============================================
// FIGHTER ENDPOINTS
// =============================================

// GET all fighters
app.get('/api/fighters', (req, res) => {
  try {
    const { search, stance, limit = 100, offset = 0 } = req.query;
    
    let sql = `
      SELECT 
        f.fighter_id,
        f.name,
        f.nickname,
        f.height,
        f.weight,
        f.reach,
        f.stance,
        f.dob,
        f.created_at
      FROM fighters f
      WHERE 1=1
    `;
    
    const params = [];
    
    // Add search filter
    if (search) {
      sql += ` AND (f.name LIKE ? OR f.nickname LIKE ?)`;
      params.push(`%${search}%`, `%${search}%`);
    }
    
    // Add stance filter
    if (stance) {
      sql += ` AND f.stance = ?`;
      params.push(stance);
    }
    
    sql += ` ORDER BY f.name ASC LIMIT ? OFFSET ?`;
    params.push(parseInt(limit), parseInt(offset));
    
    const stmt = db.prepare(sql);
    const fighters = stmt.all(...params);
    
    res.json({
      success: true,
      count: fighters.length,
      data: fighters
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET single fighter by ID
app.get('/api/fighters/:id', (req, res) => {
  try {
    const stmt = db.prepare(`
      SELECT 
        fighter_id,
        name,
        nickname,
        height,
        weight,
        reach,
        stance,
        dob,
        created_at
      FROM fighters
      WHERE fighter_id = ?
    `);
    const fighter = stmt.get(req.params.id);
    
    if (!fighter) {
      return res.status(404).json({
        success: false,
        error: 'Fighter not found'
      });
    }
    
    res.json({
      success: true,
      data: fighter
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET fighter statistics
app.get('/api/fighters/:id/stats', (req, res) => {
  try {
    const stmt = db.prepare(`
      SELECT 
        stat_id,
        fighter_id,
        slpm,
        str_acc,
        sapm,
        str_def,
        td_avg,
        td_acc,
        td_def,
        sub_avg,
        updated_at
      FROM fighter_stats
      WHERE fighter_id = ?
    `);
    const stats = stmt.get(req.params.id);
    
    if (!stats) {
      return res.status(404).json({
        success: false,
        error: 'Fighter stats not found'
      });
    }
    
    res.json({
      success: true,
      data: stats
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET fighter complete profile (fighter + stats)
app.get('/api/fighters/:id/profile', (req, res) => {
  try {
    const stmt = db.prepare(`
      SELECT 
        f.fighter_id,
        f.name,
        f.nickname,
        f.height,
        f.weight,
        f.reach,
        f.stance,
        f.dob,
        f.created_at,
        fs.slpm,
        fs.str_acc,
        fs.sapm,
        fs.str_def,
        fs.td_avg,
        fs.td_acc,
        fs.td_def,
        fs.sub_avg
      FROM fighters f
      LEFT JOIN fighter_stats fs ON f.fighter_id = fs.fighter_id
      WHERE f.fighter_id = ?
    `);
    const fighter = stmt.get(req.params.id);
    
    if (!fighter) {
      return res.status(404).json({
        success: false,
        error: 'Fighter not found'
      });
    }
    
    res.json({
      success: true,
      data: fighter
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET fighter fight history
app.get('/api/fighters/:id/fights', (req, res) => {
  try {
    const stmt = db.prepare(`
      SELECT 
        f.fight_id,
        e.event_name,
        e.event_date,
        e.location,
        CASE 
          WHEN f.fighter1_id = ? THEN f2.name
          ELSE f1.name
        END as opponent_name,
        CASE 
          WHEN f.fighter1_id = ? THEN f2.nickname
          ELSE f1.nickname
        END as opponent_nickname,
        CASE 
          WHEN f.winner_id = ? THEN 'Win'
          WHEN f.winner_id IS NULL THEN 'Draw/NC'
          ELSE 'Loss'
        END as result,
        f.method,
        f.round,
        f.time
      FROM fights f
      JOIN events e ON f.event_id = e.event_id
      JOIN fighters f1 ON f.fighter1_id = f1.fighter_id
      JOIN fighters f2 ON f.fighter2_id = f2.fighter_id
      WHERE f.fighter1_id = ? OR f.fighter2_id = ?
      ORDER BY e.event_date DESC
    `);
    const fights = stmt.all(req.params.id, req.params.id, req.params.id, req.params.id, req.params.id);
    
    res.json({
      success: true,
      count: fights.length,
      data: fights
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// =============================================
// FIGHT ENDPOINTS
// =============================================

// GET all fights
app.get('/api/fights', (req, res) => {
  try {
    const { limit = 50, offset = 0 } = req.query;
    
    const stmt = db.prepare(`
      SELECT 
        f.fight_id,
        e.event_name,
        e.event_date,
        f1.name as fighter1_name,
        f1.nickname as fighter1_nickname,
        f2.name as fighter2_name,
        f2.nickname as fighter2_nickname,
        w.name as winner_name,
        f.method,
        f.round,
        f.time
      FROM fights f
      JOIN events e ON f.event_id = e.event_id
      JOIN fighters f1 ON f.fighter1_id = f1.fighter_id
      JOIN fighters f2 ON f.fighter2_id = f2.fighter_id
      LEFT JOIN fighters w ON f.winner_id = w.fighter_id
      ORDER BY e.event_date DESC
      LIMIT ? OFFSET ?
    `);
    const fights = stmt.all(parseInt(limit), parseInt(offset));
    
    res.json({
      success: true,
      count: fights.length,
      data: fights
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET single fight by ID
app.get('/api/fights/:id', (req, res) => {
  try {
    const fightStmt = db.prepare(`
      SELECT 
        f.fight_id,
        f.event_id,
        e.event_name,
        e.event_date,
        e.location,
        f.fighter1_id,
        f1.name as fighter1_name,
        f1.nickname as fighter1_nickname,
        f1.height as fighter1_height,
        f1.weight as fighter1_weight,
        f1.reach as fighter1_reach,
        f1.stance as fighter1_stance,
        f.fighter2_id,
        f2.name as fighter2_name,
        f2.nickname as fighter2_nickname,
        f2.height as fighter2_height,
        f2.weight as fighter2_weight,
        f2.reach as fighter2_reach,
        f2.stance as fighter2_stance,
        f.winner_id,
        w.name as winner_name,
        f.method,
        f.round,
        f.time,
        f.created_at
      FROM fights f
      JOIN events e ON f.event_id = e.event_id
      JOIN fighters f1 ON f.fighter1_id = f1.fighter_id
      JOIN fighters f2 ON f.fighter2_id = f2.fighter_id
      LEFT JOIN fighters w ON f.winner_id = w.fighter_id
      WHERE f.fight_id = ?
    `);
    const fight = fightStmt.get(req.params.id);
    
    if (!fight) {
      return res.status(404).json({
        success: false,
        error: 'Fight not found'
      });
    }
    
    // Get fighter stats
    const statsStmt = db.prepare('SELECT * FROM fighter_stats WHERE fighter_id = ?');
    const fighter1Stats = statsStmt.get(fight.fighter1_id);
    const fighter2Stats = statsStmt.get(fight.fighter2_id);
    
    fight.fighter1_stats = fighter1Stats;
    fight.fighter2_stats = fighter2Stats;
    
    res.json({
      success: true,
      data: fight
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// =============================================
// STATISTICS ENDPOINTS
// =============================================

// GET database statistics
app.get('/api/stats/summary', (req, res) => {
  try {
    const eventCount = db.prepare('SELECT COUNT(*) as count FROM events').get();
    const fighterCount = db.prepare('SELECT COUNT(*) as count FROM fighters').get();
    const fightCount = db.prepare('SELECT COUNT(*) as count FROM fights').get();
    
    const recentEvents = db.prepare(`
      SELECT event_name, event_date 
      FROM events 
      ORDER BY event_date DESC 
      LIMIT 5
    `).all();
    
    res.json({
      success: true,
      data: {
        total_events: eventCount.count,
        total_fighters: fighterCount.count,
        total_fights: fightCount.count,
        recent_events: recentEvents
      }
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET fighters by stance distribution
app.get('/api/stats/stances', (req, res) => {
  try {
    const stmt = db.prepare(`
      SELECT 
        stance,
        COUNT(*) as count
      FROM fighters
      WHERE stance != 'N/A'
      GROUP BY stance
      ORDER BY count DESC
    `);
    const stances = stmt.all();
    
    res.json({
      success: true,
      data: stances
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// GET top fighters by wins
app.get('/api/stats/top-fighters', (req, res) => {
  try {
    const { limit = 10 } = req.query;
    
    const stmt = db.prepare(`
      SELECT 
        f.fighter_id,
        f.name,
        f.nickname,
        COUNT(CASE WHEN fi.winner_id = f.fighter_id THEN 1 END) as wins,
        COUNT(CASE WHEN fi.winner_id != f.fighter_id AND fi.winner_id IS NOT NULL THEN 1 END) as losses,
        COUNT(*) as total_fights
      FROM fighters f
      LEFT JOIN fights fi ON f.fighter_id = fi.fighter1_id OR f.fighter_id = fi.fighter2_id
      GROUP BY f.fighter_id, f.name, f.nickname
      HAVING total_fights > 0
      ORDER BY wins DESC, losses ASC
      LIMIT ?
    `);
    const topFighters = stmt.all(parseInt(limit));
    
    res.json({
      success: true,
      data: topFighters
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// =============================================
// ROOT & ERROR HANDLING
// =============================================

// Root endpoint
app.get('/', (req, res) => {
  res.json({
    message: 'UFC Stats API',
    version: '1.0.0',
    endpoints: {
      events: {
        'GET /api/events': 'Get all events',
        'GET /api/events/:id': 'Get event by ID',
        'GET /api/events/:id/fights': 'Get all fights for an event'
      },
      fighters: {
        'GET /api/fighters': 'Get all fighters (supports ?search=name&stance=Orthodox)',
        'GET /api/fighters/:id': 'Get fighter by ID',
        'GET /api/fighters/:id/stats': 'Get fighter statistics',
        'GET /api/fighters/:id/profile': 'Get complete fighter profile',
        'GET /api/fighters/:id/fights': 'Get fighter fight history'
      },
      fights: {
        'GET /api/fights': 'Get all fights',
        'GET /api/fights/:id': 'Get fight by ID with complete details'
      },
      statistics: {
        'GET /api/stats/summary': 'Get database summary',
        'GET /api/stats/stances': 'Get fighters by stance distribution',
        'GET /api/stats/top-fighters': 'Get top fighters by wins'
      }
    }
  });
});

// 404 handler
app.use((req, res) => {
  res.status(404).json({
    success: false,
    error: 'Endpoint not found'
  });
});

// Error handler
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).json({
    success: false,
    error: 'Internal server error'
  });
});

// Start server
app.listen(PORT, () => {
  console.log(`\n🚀 UFC Stats API running on port ${PORT}`);
  console.log(`📊 Access the API at http://localhost:${PORT}`);
  console.log(`📖 View all endpoints at http://localhost:${PORT}/\n`);
});

// Graceful shutdown
process.on('SIGINT', () => {
  db.close();
  console.log('\nDatabase connection closed');
  process.exit(0);
});