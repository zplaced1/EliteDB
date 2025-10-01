import { Database } from "bun:sqlite";

// Open the existing database
const db = new Database("galaxy.db");

console.log("Searching for systems that contain BOTH Earth-like worlds AND ringed, landable atmospheric planets...");

// Query to find systems that have Earth-like worlds (to analyze for other interesting planets)
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
    AND system_data LIKE '%"subType":"Earth-like world"%'
  ORDER BY distance_from_sol ASC
`;

const systems = db.prepare(query).all();

console.log(`Found ${systems.length} systems with Earth-like worlds. Analyzing for systems that also have ringed, landable atmospheric planets...`);

const matchingSystems: any[] = [];

systems.forEach((system: any) => {
  try {
    const systemData = JSON.parse(system.system_data);
    const bodies = systemData.bodies || [];
    
    // Find Earth-like worlds
    const earthLikeWorlds = bodies.filter((body: any) => 
      body.subType === "Earth-like world"
    );
    
    // Find ringed, landable planets with atmospheres (any type, not just Earth-like)
    const ringedLandableAtmospheric = bodies.filter((body: any) => 
      body.type === "Planet" &&
      body.rings && body.rings.length > 0 &&
      body.isLandable === true &&
      body.atmosphereType && body.atmosphereType !== null
    );
    
    // We want systems that have BOTH Earth-like worlds AND ringed landable atmospheric planets
    if (earthLikeWorlds.length > 0 && ringedLandableAtmospheric.length > 0) {
      // Calculate additional system features for context
      const features: string[] = [];
      let totalBodies = bodies.length;
      let waterWorlds = bodies.filter((b: any) => b.subType === "Water world").length;
      let ammoniaWorlds = bodies.filter((b: any) => b.subType === "Ammonia world").length;
      let ringedBodies = bodies.filter((b: any) => b.rings && b.rings.length > 0).length;
      let landableBodies = bodies.filter((b: any) => b.isLandable === true).length;
      let atmosphericBodies = bodies.filter((b: any) => b.atmosphereType && b.atmosphereType !== null).length;
      
      // Add context features
      features.push(`${earthLikeWorlds.length} Earth-like world(s)`);
      features.push(`${ringedLandableAtmospheric.length} ringed landable atmospheric planet(s)`);
      if (waterWorlds > 0) features.push(`${waterWorlds} Water world(s)`);
      if (ammoniaWorlds > 0) features.push(`${ammoniaWorlds} Ammonia world(s)`);
      if (ringedBodies > ringedLandableAtmospheric.length) features.push(`${ringedBodies} total ringed bodies`);
      if (totalBodies > 50) features.push(`${totalBodies} total bodies`);
      
      matchingSystems.push({
        ...system,
        earthLikeWorlds,
        ringedLandableAtmospheric,
        features: features.join(", "),
        totalBodies,
        waterWorlds,
        ammoniaWorlds,
        ringedBodies,
        landableBodies,
        atmosphericBodies
      });
    }
  } catch (e) {
    console.error(`Error parsing system data for ${system.system_name}:`, e);
  }
});

console.log(`\nFound ${matchingSystems.length} systems with both Earth-like worlds AND ringed, landable atmospheric planets!\n`);

if (matchingSystems.length > 0) {
  console.log("=== SYSTEMS WITH EARTH-LIKE WORLDS + RINGED LANDABLE ATMOSPHERIC PLANETS (sorted by distance from Sol) ===");
  console.log("Rank | System Name                | Distance (LY) | Total Bodies | Earth-likes | Ringed Landable | Features");
  console.log("-".repeat(135));

  matchingSystems.forEach((system: any, index: number) => {
    console.log(`${(index + 1).toString().padStart(4)} | ${system.system_name.padEnd(26)} | ${system.distance_from_sol.toFixed(2).padStart(12)} | ${(system.body_count || 0).toString().padStart(12)} | ${system.earthLikeWorlds.length.toString().padStart(11)} | ${system.ringedLandableAtmospheric.length.toString().padStart(15)} | ${system.features}`);
  });

  // Show detailed information about the closest system
  const closest = matchingSystems[0];
  console.log(`\n=== CLOSEST SYSTEM WITH BOTH EARTH-LIKE WORLDS AND RINGED LANDABLE ATMOSPHERIC PLANETS ===`);
  console.log(`System: ${closest.system_name}`);
  console.log(`Distance from Sol: ${closest.distance_from_sol.toFixed(2)} light years`);
  console.log(`Coordinates: (${closest.x.toFixed(1)}, ${closest.y.toFixed(1)}, ${closest.z.toFixed(1)})`);
  console.log(`Total bodies in system: ${closest.body_count || 0}`);
  
  console.log(`\nEarth-like worlds:`);
  closest.earthLikeWorlds.forEach((body: any, idx: number) => {
    console.log(`  ${idx + 1}. ${body.name} (${body.subType})`);
    if (body.isLandable) console.log(`     - Landable: Yes`);
    if (body.atmosphereType) console.log(`     - Atmosphere: ${body.atmosphereType}`);
  });
  
  console.log(`\nRinged, landable atmospheric planets:`);
  closest.ringedLandableAtmospheric.forEach((body: any, idx: number) => {
    console.log(`  ${idx + 1}. ${body.name} (${body.subType || 'Planet'})`);
    console.log(`     - Atmosphere: ${body.atmosphereType}`);
    console.log(`     - Rings: ${body.rings.length} ring system(s)`);
    console.log(`     - Landable: Yes`);
    if (body.gravity) console.log(`     - Gravity: ${body.gravity.toFixed(2)}g`);
    if (body.surfaceTemperature) console.log(`     - Surface Temperature: ${body.surfaceTemperature}K`);
    if (body.surfacePressure) console.log(`     - Surface Pressure: ${body.surfacePressure} atm`);
    console.log();
  });

  // Show top 15 closest systems with brief details
  if (matchingSystems.length > 1) {
    console.log("=== TOP 15 CLOSEST SYSTEMS ===");
    matchingSystems.slice(0, 15).forEach((system: any, index: number) => {
      console.log(`${(index + 1).toString().padStart(2)}. ${system.system_name.padEnd(30)} - ${system.distance_from_sol.toFixed(2).padStart(8)} LY - ${(system.body_count || 0).toString().padStart(3)} bodies - ${system.earthLikeWorlds.length} Earth-like + ${system.ringedLandableAtmospheric.length} ringed landable atmospheric`);
    });
  }

  // Create a table for quick access to these special systems
  console.log("\nCreating 'earthlike_plus_ringed_landable_systems' table...");
  db.run(`DROP TABLE IF EXISTS earthlike_plus_ringed_landable_systems`);
  db.run(`
    CREATE TABLE earthlike_plus_ringed_landable_systems (
      id INTEGER,
      system_name TEXT,
      x REAL, y REAL, z REAL,
      distance_from_sol REAL,
      body_count INTEGER,
      earthlike_count INTEGER,
      ringed_landable_atmospheric_count INTEGER,
      water_world_count INTEGER,
      ammonia_world_count INTEGER,
      total_ringed_bodies INTEGER,
      total_landable_bodies INTEGER,
      total_atmospheric_bodies INTEGER,
      features TEXT,
      matched_body_name TEXT,
      system_data TEXT
    )
  `);

  const insertStmt = db.prepare(`
    INSERT INTO earthlike_plus_ringed_landable_systems VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  `);

  matchingSystems.forEach(system => {
    insertStmt.run(
      system.id,
      system.system_name,
      system.x, system.y, system.z,
      system.distance_from_sol,
      system.body_count || 0,
      system.earthLikeWorlds.length,
      system.ringedLandableAtmospheric.length,
      system.waterWorlds,
      system.ammoniaWorlds,
      system.ringedBodies,
      system.landableBodies,
      system.atmosphericBodies,
      system.features,
      system.matched_body_name,
      system.system_data
    );
  });

  console.log("Created 'earthlike_plus_ringed_landable_systems' table with detailed information.");
  console.log("\nYou can query this table to find these special systems:");
  console.log("SELECT system_name, distance_from_sol, earthlike_count, ringed_landable_atmospheric_count FROM earthlike_plus_ringed_landable_systems ORDER BY distance_from_sol;");
  
} else {
  console.log("No systems found with both Earth-like worlds and ringed, landable atmospheric planets.");
  console.log("Let's try a broader search...");
  
  // Fallback: Just find systems with ringed landable atmospheric planets (any type)
  console.log("\n=== SEARCHING FOR SYSTEMS WITH ANY RINGED, LANDABLE ATMOSPHERIC PLANETS ===");
  
  const broadQuery = `
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
    LIMIT 1000
  `;
  
  const allSystems = db.prepare(broadQuery).all();
  const broadMatches: any[] = [];
  
  allSystems.forEach((system: any) => {
    try {
      const systemData = JSON.parse(system.system_data);
      const bodies = systemData.bodies || [];
      
      const ringedLandableAtmospheric = bodies.filter((body: any) => 
        body.type === "Planet" &&
        body.rings && body.rings.length > 0 &&
        body.isLandable === true &&
        body.atmosphereType && body.atmosphereType !== null
      );
      
      if (ringedLandableAtmospheric.length > 0) {
        broadMatches.push({
          ...system,
          ringedLandableAtmospheric,
          planetTypes: ringedLandableAtmospheric.map(b => b.subType || 'Unknown').join(', ')
        });
      }
    } catch (e) {
      // Skip parsing errors
    }
  });
  
  console.log(`Found ${broadMatches.length} systems with ringed, landable atmospheric planets:`);
  broadMatches.slice(0, 10).forEach((system: any, index: number) => {
    console.log(`${(index + 1).toString().padStart(2)}. ${system.system_name.padEnd(30)} - ${system.distance_from_sol.toFixed(2).padStart(8)} LY - ${(system.body_count || 0).toString().padStart(3)} bodies - ${system.ringedLandableAtmospheric.length} planet(s): ${system.planetTypes}`);
  });
}

db.close();
