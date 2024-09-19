import Database from 'better-sqlite3';

/**
 * GET handler to retrieve the average mandate count per state.
 *
 * @param {Request} request - The incoming HTTP request.
 * @returns {Promise<Response>} - The HTTP response containing the average mandates per state or an error message.
 */
export async function GET() {
  try {
    // Open the SQLite database in read-only mode
    const db = new Database('counties_optimized.db', { readonly: true });

    // Prepare the SQL statement to calculate the average mandate count per state
    const stmt = db.prepare(`
      SELECT
        states.name AS stateName,
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
      INNER JOIN
        states ON county_mandate_counts.state_id = states.id
      GROUP BY
        states.id
      ORDER BY
        averageMandateCount DESC;
    `);

    // Execute the query
    const averageMandateData = stmt.all();

    // Close the database connection
    db.close();

    // Check if data is returned
    if (averageMandateData.length === 0) {
      return new Response(
        JSON.stringify({ error: 'No mask mandate data found.' }),
        { status: 404, headers: { 'Content-Type': 'application/json' } }
      );
    }

    // Structure the response data
    const responseData = averageMandateData.map(item => ({
      state: item.stateName,
      average_mandate_count: parseFloat(item.averageMandateCount.toFixed(2))
    }));

    // Return the successful response with the data
    return new Response(
      JSON.stringify({ average_mandates_per_state: responseData }),
      { status: 200, headers: { 'Content-Type': 'application/json' } }
    );

  } catch (error) {
    // Log the error for debugging purposes
    console.error('Database error:', error);

    // Return a 500 Internal Server Error response
    return new Response(
      JSON.stringify({ error: 'Internal server error.' }),
      { status: 500, headers: { 'Content-Type': 'application/json' } }
    );
  }
}
