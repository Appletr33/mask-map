// maskMandates.js

import Database from 'better-sqlite3';

/**
 * GET handler to retrieve the number of mask mandates per county for a given state.
 * Only includes counties where face masks are required in public (face_masks_required = 1).
 * 
 * @param {Request} request - The incoming HTTP request.
 * @returns {Promise<Response>} - The HTTP response containing the mandate counts or an error message.
 */
export async function GET(request) {
  // Parse the query parameters from the request URL
  const { searchParams } = new URL(request.url);
  const state = decodeURIComponent(searchParams.get('state')).trim();

  // Validate the 'state' parameter
  if (!state) {
    return new Response(
      JSON.stringify({ error: 'State parameter is required.' }),
      { status: 400, headers: { 'Content-Type': 'application/json' } }
    );
  }
  
  try {
    // Open the SQLite database in read-only mode
    const db = new Database('counties_optimized.db', { readonly: true });
  
    // First, retrieve the state_id from the states table based on the state name
    const getStateIdStmt = db.prepare(`
      SELECT id FROM states WHERE TRIM(LOWER(name)) = TRIM(LOWER(?))
    `);
    const stateData = getStateIdStmt.get(state);
  
    // Check if stateData is found
    if (!stateData || !stateData.id) {
      return new Response(
        JSON.stringify({ error: `State not found: ${state}` }),
        { status: 404, headers: { 'Content-Type': 'application/json' } }
      );
    }
  
    const stateId = stateData.id; // Extract the state_id
  
    // Prepare the SQL statement to count mask mandates per county where face_masks_required = 1
    const stmt = db.prepare(`
      SELECT
        counties.county_name AS countyName,
        COUNT(counties.id) AS mandateCount
      FROM
        counties
      WHERE
        counties.state_id = ?
        AND counties.face_masks_required = 1
      GROUP BY
        counties.county_name
      ORDER BY
        mandateCount DESC, countyName ASC
    `);
  
    // Execute the query with the retrieved state_id
    const mandateData = stmt.all(stateId);
  
    // Check if any data is returned
    if (mandateData.length === 0) {
      return new Response(
        JSON.stringify({ error: `No mask mandate data found for state: ${state}` }),
        { status: 404, headers: { 'Content-Type': 'application/json' } }
      );
    }
  
    // Respond with the mandate data
    return new Response(
      JSON.stringify({ state, mandates: mandateData }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );
  
  } catch (error) {
    console.error('Database error:', error);
    return new Response(
      JSON.stringify({ error: 'Internal server error.' }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }
}