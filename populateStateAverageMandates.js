const Database = require('better-sqlite3');
const path = require('path');

// Path to your SQLite database
const dbPath = path.join(__dirname, 'counties_optimized.db');
console.log(dbPath);

// Initialize the SQLite database
const db = new Database(dbPath, { readonly: false });

try {
  // Create the state_average_mandates table if it doesn't exist
  db.exec(`
    CREATE TABLE IF NOT EXISTS state_average_mandates (
      state_id INTEGER PRIMARY KEY,
      state_name TEXT UNIQUE,
      average_mandate_count REAL,
      FOREIGN KEY(state_id) REFERENCES states(id)
    );
  `);

  // Prepare statements
  const getStatesStmt = db.prepare(`SELECT id, name FROM states`);
  
  const calculateAverageStmt = db.prepare(`
    SELECT
      county_mandate_counts.state_id,
      AVG(county_mandate_counts.mandateCount) AS averageMandateCount
    FROM (
      SELECT 
        state_id,
        COUNT(*) AS mandateCount
      FROM 
        counties
      WHERE 
        face_masks_required = 1
      GROUP BY 
        state_id, county_name
    ) AS county_mandate_counts
    GROUP BY county_mandate_counts.state_id;
  `);

  // Prepare an insert or update statement
  const insertOrUpdateStmt = db.prepare(`
    INSERT INTO state_average_mandates (state_id, state_name, average_mandate_count)
    VALUES (@state_id, @state_name, @average_mandate_count)
    ON CONFLICT(state_id) DO UPDATE SET
      average_mandate_count = excluded.average_mandate_count,
      state_name = excluded.state_name
  `);

  // Fetch all states
  const states = getStatesStmt.all();

  // Calculate average mandates per state
  const averageMandateData = calculateAverageStmt.all();

  // Map state_id to state_name for quick lookup
  const stateIdToName = {};
  states.forEach(state => {
    stateIdToName[state.id] = state.name;
  });

  // Insert or update the average mandates into the new table
  const insertMany = db.transaction((data) => {
    data.forEach(item => {
      // Only insert or update if the state_id exists in the stateIdToName mapping
      if (stateIdToName[item.state_id]) {
        insertOrUpdateStmt.run({
          state_id: item.state_id,
          state_name: stateIdToName[item.state_id],
          average_mandate_count: item.averageMandateCount
        });
      }
    });
  });

  insertMany(averageMandateData);

  console.log('state_average_mandates table has been successfully populated.');

} catch (error) {
  console.error('Error populating state_average_mandates table:', error);
} finally {
  db.close();
}
