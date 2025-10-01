import { Database } from "bun:sqlite";

// Open the existing database
const db = new Database("galaxy.db");

// Query to get all systems sorted by distance from Sol (0,0,0)
// Using 3D Euclidean distance: sqrt(x² + y² + z²)
const query = `
  SELECT 
    id,
    system_name,
    x, y, z,
    matched_body_name,
    sqrt(x*x + y*y + z*z) as distance_from_sol
  FROM systems 
  WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
  ORDER BY distance_from_sol ASC
`;

console.log("Fetching systems sorted by distance from Sol...");

const systems = db.prepare(query).all();

console.log(`Found ${systems.length} systems with coordinates.`);
console.log("\nClosest systems to Sol:");
console.log("Rank | System Name | Distance (LY) | Coordinates | Planet Name");
console.log("-".repeat(80));

systems.slice(0, 20).forEach((system: any, index: number) => {
  console.log(`${(index + 1).toString().padStart(4)} | ${system.system_name.padEnd(25)} | ${system.distance_from_sol.toFixed(2).padStart(10)} | (${system.x.toFixed(1)}, ${system.y.toFixed(1)}, ${system.z.toFixed(1)}) | ${system.matched_body_name}`);
});

if (systems.length > 20) {
  console.log("\nFarthest systems from Sol:");
  console.log("Rank | System Name | Distance (LY) | Coordinates | Planet Name");
  console.log("-".repeat(80));
  
  systems.slice(-10).forEach((system: any, index: number) => {
    const rank = systems.length - 9 + index;
    console.log(`${rank.toString().padStart(4)} | ${system.system_name.padEnd(25)} | ${system.distance_from_sol.toFixed(2).padStart(10)} | (${system.x.toFixed(1)}, ${system.y.toFixed(1)}, ${system.z.toFixed(1)}) | ${system.matched_body_name}`);
  });
}

// Optionally save to a new table for quick access
console.log("\nCreating sorted table for quick access...");
db.run(`DROP TABLE IF EXISTS systems_by_distance`);
db.run(`
  CREATE TABLE systems_by_distance AS
  SELECT 
    ROW_NUMBER() OVER (ORDER BY sqrt(x*x + y*y + z*z)) as rank,
    id,
    system_name,
    x, y, z,
    matched_body_name,
    sqrt(x*x + y*y + z*z) as distance_from_sol,
    matched_body,
    system_data
  FROM systems 
  WHERE x IS NOT NULL AND y IS NOT NULL AND z IS NOT NULL
  ORDER BY distance_from_sol ASC
`);

console.log("Created 'systems_by_distance' table with ranking.");
db.close();
