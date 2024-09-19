// csv-to-sqlite-optimized-fixed.js

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');
const Database = require('better-sqlite3');

// Path to your CSV file
const csvFilePath = path.join(__dirname, '../', 'data.csv');

// Initialize SQLite database
const db = new Database('counties_optimized.db');

// Enable foreign key constraints
db.pragma('foreign_keys = ON');

// Create normalized tables
db.exec(`
  DROP TABLE IF EXISTS county_citations;
  DROP TABLE IF EXISTS citations;
  DROP TABLE IF EXISTS counties;
  DROP TABLE IF EXISTS masks_orders;
  DROP TABLE IF EXISTS states;
  
  CREATE TABLE states (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE
  );
  
  CREATE TABLE masks_orders (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code INTEGER UNIQUE
  );
  
  CREATE TABLE citations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    citation TEXT UNIQUE
  );
  
  CREATE TABLE counties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_id INTEGER,
    county_name TEXT,
    fips_code INTEGER,
    date INTEGER, -- Unix timestamp
    masks_order_id INTEGER,
    face_masks_required INTEGER, -- 0 for No, 1 for Yes
    FOREIGN KEY(state_id) REFERENCES states(id),
    FOREIGN KEY(masks_order_id) REFERENCES masks_orders(id)
  );
  
  CREATE TABLE county_citations (
    county_id INTEGER,
    citation_id INTEGER,
    FOREIGN KEY(county_id) REFERENCES counties(id),
    FOREIGN KEY(citation_id) REFERENCES citations(id),
    PRIMARY KEY (county_id, citation_id)
  );
  
  -- Indexes for faster lookups
  CREATE INDEX idx_state ON states(name);
  CREATE INDEX idx_masks_order ON masks_orders(code);
  CREATE INDEX idx_citations ON citations(citation);
  CREATE INDEX idx_county_state ON counties(state_id);
`);

// Prepare statements for inserting data
const insertState = db.prepare(`
  INSERT OR IGNORE INTO states (name) VALUES (?)
`);
const getStateId = db.prepare(`
  SELECT id FROM states WHERE name = ?
`);

const insertMasksOrder = db.prepare(`
  INSERT OR IGNORE INTO masks_orders (code) VALUES (?)
`);
const getMasksOrderId = db.prepare(`
  SELECT id FROM masks_orders WHERE code = ?
`);

const insertCitation = db.prepare(`
  INSERT OR IGNORE INTO citations (citation) VALUES (?)
`);
const getCitationId = db.prepare(`
  SELECT id FROM citations WHERE citation = ?
`);

const insertCounty = db.prepare(`
  INSERT INTO counties (
    state_id,
    county_name,
    fips_code,
    date,
    masks_order_id,
    face_masks_required
  ) VALUES (?, ?, ?, ?, ?, ?)
`);
const getLastCountyId = db.prepare(`
  SELECT last_insert_rowid() as id
`);

const insertCountyCitation = db.prepare(`
  INSERT OR IGNORE INTO county_citations (county_id, citation_id) VALUES (?, ?)
`);

// Create transactions for batch inserts
const insertStates = db.transaction((states) => {
  for (const state of states) {
    insertState.run(state);
  }
});

const insertMasksOrders = db.transaction((masksOrders) => {
  for (const code of masksOrders) {
    insertMasksOrder.run(code);
  }
});

const insertCitations = db.transaction((citations) => {
  for (const citation of citations) {
    insertCitation.run(citation);
  }
});

const insertCounties = db.transaction((counties) => {
  for (const county of counties) {
    insertCounty.run(
      county.state_id,
      county.county_name,
      county.fips_code,
      county.date,
      county.masks_order_id,
      county.face_masks_required
    );
  }
});

const insertCountyCitations = db.transaction((countyCitations) => {
  for (const cc of countyCitations) {
    insertCountyCitation.run(cc.county_id, cc.citation_id);
  }
});

// Caches to store already processed entries
const stateCache = new Map();
const masksOrderCache = new Map();
const citationCache = new Map();

// Batch variables
const batchSize = 1000;
let countyBatch = [];
let countyCitationsBatch = [];
let totalRows = 0;
let startTime = Date.now();

// Function to parse and optimize row data
function parseRow(row) {
  // Handle State_Tribe_Territory
  let stateId;
  if (stateCache.has(row.State_Tribe_Territory)) {
    stateId = stateCache.get(row.State_Tribe_Territory);
  } else {
    insertState.run(row.State_Tribe_Territory);
    const state = getStateId.get(row.State_Tribe_Territory);
    stateId = state.id;
    stateCache.set(row.State_Tribe_Territory, stateId);
  }

  // Handle Masks_Order_Code
  let masksOrderId;
  const masksOrderCode = parseInt(row.Masks_Order_Code, 10);
  if (masksOrderCache.has(masksOrderCode)) {
    masksOrderId = masksOrderCache.get(masksOrderCode);
  } else {
    insertMasksOrder.run(masksOrderCode);
    const masksOrder = getMasksOrderId.get(masksOrderCode);
    masksOrderId = masksOrder.id;
    masksOrderCache.set(masksOrderCode, masksOrderId);
  }

  // Handle Face_Masks_Required_in_Public
  const faceMasksRequired = row.Face_Masks_Required_in_Public.trim().toLowerCase().includes('yes') || row.Face_Masks_Required_in_Public.trim().toLowerCase().includes('public mask mandate') ? 1 : 0;

  // Handle Date - convert to Unix timestamp
  const date = Math.floor(new Date(row.Date).getTime() / 1000);

  // Handle FIPS_Code
  const fipsCode = parseInt(row.FIPS_Code, 10);

  // Handle Citations
  // Remove brackets and split by comma, then trim
  const citations = row.Citations.replace(/^\[|\]$/g, '').split(',').map(c => c.trim()).filter(c => c.length > 0);

  // Insert citations and get their IDs
  const citationIds = citations.map(citation => {
    if (citationCache.has(citation)) {
      return citationCache.get(citation);
    } else {
      insertCitation.run(citation);
      const cit = getCitationId.get(citation);
      citationCache.set(citation, cit.id);
      return cit.id;
    }
  });

  return {
    state_id: stateId,
    county_name: row.County_Name,
    fips_code: fipsCode,
    date: date,
    masks_order_id: masksOrderId,
    face_masks_required: faceMasksRequired,
    citation_ids: citationIds
  };
}

// Read and process the CSV file
const stream = fs.createReadStream(csvFilePath)
  .pipe(csv())
  .on('data', (row) => {
    try {
      totalRows++;
      const parsed = parseRow(row);
      countyBatch.push({
        state_id: parsed.state_id,
        county_name: parsed.county_name,
        fips_code: parsed.fips_code,
        date: parsed.date,
        masks_order_id: parsed.masks_order_id,
        face_masks_required: parsed.face_masks_required,
        citation_ids: parsed.citation_ids
      });

      // Insert in batches
      if (countyBatch.length === batchSize) {
        // Insert counties
        insertCounties(countyBatch);

        // Fetch all newly inserted county IDs
        const lastId = getLastCountyId.get().id;
        const firstId = lastId - batchSize + 1;

        // Prepare county-citations for this batch
        const currentBatchSize = countyBatch.length;
        for (let i = 0; i < currentBatchSize; i++) {
          const county = countyBatch[i];
          const countyId = firstId + i;
          for (const citation_id of county.citation_ids) {
            countyCitationsBatch.push({
              county_id: countyId,
              citation_id: citation_id
            });
          }
        }

        // Insert county-citations
        insertCountyCitations(countyCitationsBatch);

        // Reset batches
        countyBatch = [];
        countyCitationsBatch = [];
      }

      // Log progress every 10,000 rows
      if (totalRows % 10000 === 0) {
        const elapsedTime = (Date.now() - startTime) / 1000;
        console.log(`Inserted ${totalRows} rows in ${elapsedTime.toFixed(2)} seconds`);
      }
    } catch (error) {
      console.error(`Error processing row ${totalRows}:`, error);
    }
  })
  .on('end', () => {
    try {
      // Insert any remaining counties
      if (countyBatch.length > 0) {
        insertCounties(countyBatch);

        // Fetch all newly inserted county IDs
        const lastId = getLastCountyId.get().id;
        const firstId = lastId - countyBatch.length + 1;

        // Prepare county-citations for this batch
        const currentBatchSize = countyBatch.length;
        for (let i = 0; i < currentBatchSize; i++) {
          const county = countyBatch[i];
          const countyId = firstId + i;
          for (const citation_id of county.citation_ids) {
            countyCitationsBatch.push({
              county_id: countyId,
              citation_id: citation_id
            });
          }
        }

        // Insert county-citations
        insertCountyCitations(countyCitationsBatch);
      }

      const elapsedTime = (Date.now() - startTime) / 1000;
      console.log(`CSV file successfully processed and data inserted into optimized SQLite database.`);
      console.log(`Total rows inserted: ${totalRows} in ${elapsedTime.toFixed(2)} seconds`);
      db.close();
    } catch (error) {
      console.error('Error during final batch insertion:', error);
      db.close();
    }
  })
  .on('error', (error) => {
    console.error('Error reading CSV file:', error);
    db.close();
  });