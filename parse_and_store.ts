import { Database } from "bun:sqlite";
import { createReadStream } from "fs";
import readline from "readline";

// Open or create the SQLite database with maximum performance optimizations
const db = new Database("galaxy.db");

// Maximum performance SQLite settings
db.run("PRAGMA journal_mode = OFF");        // Disable journaling for max speed (less safe)
db.run("PRAGMA synchronous = OFF");         // No disk syncing (fastest, less safe)
db.run("PRAGMA cache_size = 2000000");      // 2GB cache
db.run("PRAGMA temp_store = memory");       // Keep temp data in RAM
db.run("PRAGMA mmap_size = 8589934592");    // 8GB memory mapping
db.run("PRAGMA page_size = 65536");         // Larger pages
db.run("PRAGMA locking_mode = EXCLUSIVE");  // Exclusive access
db.run("PRAGMA count_changes = OFF");       // Disable change counting
db.run("PRAGMA auto_vacuum = NONE");        // Disable auto vacuum

db.run(`
  CREATE TABLE IF NOT EXISTS systems (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    system_name TEXT,
    x REAL,
    y REAL,
    z REAL,
    matched_body_name TEXT,
    matched_body TEXT,
    system_data TEXT
  );
`);

// Prepare the statement for better performance
const insertStmt = db.prepare(
  "INSERT INTO systems (system_name, x, y, z, matched_body_name, matched_body, system_data) VALUES (?, ?, ?, ?, ?, ?, ?)"
);

async function processFile() {
  const fileStream = createReadStream("galaxy_1month.json", { 
    highWaterMark: 1024 * 1024 * 2 // 2MB buffer chunks
  });
  const rl = readline.createInterface({ 
    input: fileStream,
    crlfDelay: Infinity 
  });

  let buffer = "";
  let insideArray = false;
  let processed = 0;
  let batchCount = 0;
  let totalChecked = 0;
  
  // Pre-compile regex for faster string operations
  const commaRegex = /,$/;
  
  // Begin transaction
  db.run("BEGIN TRANSACTION");
  
  for await (const line of rl) {
    const trimmed = line.trim();
    if (trimmed === "[") { insideArray = true; continue; }
    if (trimmed === "]") break;
    if (!insideArray) continue;
    
    // Faster comma removal
    buffer += trimmed.replace(commaRegex, "");
    
    try {
      const obj = JSON.parse(buffer);
      buffer = "";
      totalChecked++;
      
      // Ultra-fast early exits with strict checks
      if (!obj.bodies || obj.population !== 0 || !obj.coords) continue;
      
      // Quick body scanning - exit as soon as we find a match
      let foundMatch = false;
      for (let i = 0; i < obj.bodies.length && !foundMatch; i++) {
        const body = obj.bodies[i];
        
        // Fastest possible checks - no fancy conditions
        if (body.rings && body.rings.length > 0 && 
            body.isLandable === true && 
            body.atmosphereType && body.atmosphereType !== null) {
          
          // Minimal object creation
          insertStmt.run(
            obj.name,
            obj.coords.x,
            obj.coords.y,
            obj.coords.z,
            body.name,
            JSON.stringify(body),
            JSON.stringify(obj)
          );
          
          processed++;
          batchCount++;
          foundMatch = true; // Exit inner loop immediately
          
          // Smaller batch commits for better memory usage
          if (batchCount >= 500) {
            db.run("COMMIT");
            db.run("BEGIN TRANSACTION");
            batchCount = 0;
          }
          
          // Less frequent logging
          if (processed % 500 === 0) {
            console.log(`Committed batch - Processed: ${processed}, Checked: ${totalChecked}, Match rate: ${(processed/totalChecked*100).toFixed(3)}%`);
          }
        }
      }
    } catch (e) {
      // More efficient buffering - only add space if buffer isn't empty
      if (buffer) buffer += " ";
    }
  }
  
  // Final commit
  db.run("COMMIT");
  console.log(`Final - Total processed: ${processed}, Total checked: ${totalChecked}, Match rate: ${(processed/totalChecked*100).toFixed(3)}%`);
  
  // Analyze database after processing
  db.run("ANALYZE");
  db.close();
}

processFile();
