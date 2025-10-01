use duckdb::{Connection, Result};
use std::time::Instant;

fn main() -> Result<(), anyhow::Error> {
    let start = Instant::now();
    
    let conn = Connection::open_in_memory()?;
    
    conn.execute_batch("
        SET memory_limit = '24GB';
        SET threads = 12;
        SET max_memory = '24GB';
        SET temp_directory = 'C:/temp';
        PRAGMA enable_profiling;
    ")?;
    
    println!("📊 Reading and filtering JSON file directly...");
    
    // DuckDB can read JSON files directly and filter in one pass!
    // This is MUCH faster than parsing JSON in application code
    conn.execute("
        CREATE TABLE raw_systems AS 
        SELECT 
            name,
            coords,
            population,
            bodies,
            bodyCount
        FROM read_json_auto('galaxy_1month.json')
        WHERE population = 0 
          AND bodies IS NOT NULL
          AND coords IS NOT NULL
    ", [])?;
    
    println!("Extracting systems with landable ringed planets with atmospheres...");
    
    conn.execute("
        CREATE TABLE matching_systems AS
        SELECT DISTINCT
            name as system_name,
            coords.x as x,
            coords.y as y, 
            coords.z as z,
            bodyCount as body_count,
            sqrt(coords.x * coords.x + coords.y * coords.y + coords.z * coords.z) as distance_from_sol,
            bodies,
            -- Extract first matching body
            (
                SELECT body
                FROM unnest(bodies) as t(body)
                WHERE body.rings IS NOT NULL 
                  AND len(body.rings) > 0
                  AND body.isLandable = true
                  AND body.atmosphereType IS NOT NULL
                LIMIT 1
            ) as matched_body
        FROM raw_systems
        WHERE EXISTS (
            SELECT 1 
            FROM unnest(bodies) as t(body)
            WHERE body.rings IS NOT NULL 
              AND len(body.rings) > 0
              AND body.isLandable = true
              AND body.atmosphereType IS NOT NULL
        )
        ORDER BY distance_from_sol ASC
    ", [])?;
    
    println!("Creating optimized tables and indexes...");
    
    // Create final optimized table
    conn.execute("
        CREATE TABLE systems AS
        SELECT 
            system_name,
            x, y, z,
            distance_from_sol,
            body_count,
            matched_body.name as matched_body_name,
            matched_body,
            bodies as system_data
        FROM matching_systems
    ", [])?;
    
    // Add indexes for fast queries
    conn.execute("CREATE INDEX idx_distance ON systems(distance_from_sol)", [])?;
    conn.execute("CREATE INDEX idx_body_count ON systems(body_count DESC)", [])?;
    conn.execute("CREATE INDEX idx_system_name ON systems(system_name)", [])?;
    
    // Get statistics
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM systems")?;
    let total_systems: i64 = stmt.query_row([], |row| row.get(0))?;
    
    let mut stmt = conn.prepare("SELECT COUNT(*) FROM raw_systems")?;
    let total_checked: i64 = stmt.query_row([], |row| row.get(0))?;
    
    let elapsed = start.elapsed();
    
    println!("\n🎯 RESULTS:");
    println!("Total systems checked: {}", total_checked);
    println!("Matching systems found: {}", total_systems);
    println!("Match rate: {:.3}%", (total_systems as f64 / total_checked as f64) * 100.0);
    println!("Total time: {:.2}s", elapsed.as_secs_f64());
    println!("Processing rate: {:.0} systems/second", total_checked as f64 / elapsed.as_secs_f64());
    
    println!("Saving to single DuckDB file: galaxy_results.db");
    
    // Use COPY TO to save as a single file database
    conn.execute("COPY systems TO 'galaxy_results.parquet'", [])?;
    
    // Create a new file-based database
    let file_db = Connection::open("galaxy_results.db")?;
    
    // Load the data and create indexes
    file_db.execute("CREATE TABLE systems AS SELECT * FROM read_parquet('galaxy_results.parquet')", [])?;
    file_db.execute("CREATE INDEX idx_distance ON systems(distance_from_sol)", [])?;
    file_db.execute("CREATE INDEX idx_body_count ON systems(body_count DESC)", [])?;
    file_db.execute("CREATE INDEX idx_system_name ON systems(system_name)", [])?;
    
    // Clean up temporary file
    std::fs::remove_file("galaxy_results.parquet").ok();
    
    println!("COMPLETE! Database saved as single file: galaxy_results.db");
    println!("You can now query with: duckdb galaxy_results.db");
    println!("Example: SELECT COUNT(*) FROM systems;");
    
    Ok(())
}