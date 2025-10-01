import { Database } from "bun:sqlite";

// Open the existing database
const db = new Database("galaxy.db");

// Query to get all systems with body count, sorted by distance from Sol
const query = `
  SELECT 
    id,
    system_name,
    x, y, z,
    matched_body_name,
    sqrt(x*x + y*y + z*z) as distance_from_sol,
    json_extract(system_data, '$.bodyCount') as body_count,
    system_data
  FROM systems 
  WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
  ORDER BY distance_from_sol ASC
`;

console.log("Fetching systems sorted by distance from Sol with body counts...");

const systems = db.prepare(query).all();

console.log(`Found ${systems.length} systems with coordinates.`);

// Sort by body count (most bodies first) for ranking
const systemsByBodies = [...systems].sort((a: any, b: any) => (b.body_count || 0) - (a.body_count || 0));

console.log("\n=== CLOSEST SYSTEMS TO SOL ===");
console.log("Rank | System Name | Distance (LY) | Bodies | Planet Name");
console.log("-".repeat(85));

systems.slice(0, 20).forEach((system: any, index: number) => {
  console.log(`${(index + 1).toString().padStart(4)} | ${system.system_name.padEnd(25)} | ${system.distance_from_sol.toFixed(2).padStart(10)} | ${(system.body_count || 0).toString().padStart(6)} | ${system.matched_body_name}`);
});

console.log("\n=== SYSTEMS WITH MOST BODIES (Top 20) ===");
console.log("Rank | System Name | Distance (LY) | Bodies | Planet Name");
console.log("-".repeat(85));

systemsByBodies.slice(0, 20).forEach((system: any, index: number) => {
  console.log(`${(index + 1).toString().padStart(4)} | ${system.system_name.padEnd(25)} | ${system.distance_from_sol.toFixed(2).padStart(10)} | ${(system.body_count || 0).toString().padStart(6)} | ${system.matched_body_name}`);
});

console.log("\n=== CLOSEST SYSTEMS WITH MANY BODIES (>50 bodies, closest first) ===");
const closestWithManyBodies = systems.filter((s: any) => (s.body_count || 0) > 50);
console.log("Rank | System Name | Distance (LY) | Bodies | Planet Name");
console.log("-".repeat(85));

closestWithManyBodies.slice(0, 15).forEach((system: any, index: number) => {
  console.log(`${(index + 1).toString().padStart(4)} | ${system.system_name.padEnd(25)} | ${system.distance_from_sol.toFixed(2).padStart(10)} | ${(system.body_count || 0).toString().padStart(6)} | ${system.matched_body_name}`);
});

// Create tables for different rankings
console.log("\nCreating ranking tables...");

// Table sorted by distance (closest first)
db.run(`DROP TABLE IF EXISTS systems_by_distance_with_bodies`);
db.run(`
  CREATE TABLE systems_by_distance_with_bodies AS
  SELECT 
    ROW_NUMBER() OVER (ORDER BY sqrt(x*x + y*y + z*z)) as distance_rank,
    id,
    system_name,
    x, y, z,
    matched_body_name,
    sqrt(x*x + y*y + z*z) as distance_from_sol,
    json_extract(system_data, '$.bodyCount') as body_count,
    matched_body,
    system_data
  FROM systems 
  WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
  ORDER BY distance_from_sol ASC
`);

// Table sorted by body count (most bodies first)
db.run(`DROP TABLE IF EXISTS systems_by_body_count`);
db.run(`
  CREATE TABLE systems_by_body_count AS
  SELECT 
    ROW_NUMBER() OVER (ORDER BY json_extract(system_data, '$.bodyCount') DESC) as body_rank,
    id,
    system_name,
    x, y, z,
    matched_body_name,
    sqrt(x*x + y*y + z*z) as distance_from_sol,
    json_extract(system_data, '$.bodyCount') as body_count,
    matched_body,
    system_data
  FROM systems 
  WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
  ORDER BY json_extract(system_data, '$.bodyCount') DESC
`);

// Combined ranking (weighted score: closer = better, more bodies = better)
db.run(`DROP TABLE IF EXISTS systems_combined_ranking`);
db.run(`
  CREATE TABLE systems_combined_ranking AS
  SELECT 
    *,
    -- Combined score: lower is better (closer + fewer bodies penalty)
    (distance_from_sol / 1000.0) + (100.0 / COALESCE(body_count, 1)) as combined_score,
    ROW_NUMBER() OVER (ORDER BY (distance_from_sol / 1000.0) + (100.0 / COALESCE(json_extract(system_data, '$.bodyCount'), 1))) as combined_rank
  FROM (
    SELECT 
      id,
      system_name,
      x, y, z,
      matched_body_name,
      sqrt(x*x + y*y + z*z) as distance_from_sol,
      json_extract(system_data, '$.bodyCount') as body_count,
      matched_body,
      system_data
    FROM systems 
    WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
  )
  ORDER BY combined_score ASC
`);

console.log("Created ranking tables:");
console.log("- systems_by_distance_with_bodies (closest first)");
console.log("- systems_by_body_count (most bodies first)"); 
console.log("- systems_combined_ranking (best balance of close + many bodies)");

db.close();
