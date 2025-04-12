import { Client } from "pg";

export async function recreateReplicationSlot(client: Client, slotName: string) {
  // Drop replication slot if it exists
  try {
    await client.query(`SELECT pg_drop_replication_slot($1)`, [slotName]);
    console.log(`Replication slot '${slotName}' dropped.`);
  } catch (error) {
    console.log(`Replication slot '${slotName}' does not exist or could not be dropped.`);
  }

  // Create replication slot
  await client.query(`
    SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')
  `, [slotName]);
  console.log(`Replication slot '${slotName}' created.`);
}

export async function recreatePublication(client: Client, publicationName: string) {
  // Drop publication if it exists
  try {
    await client.query(`DROP PUBLICATION IF EXISTS ${publicationName}`);
    console.log(`Publication '${publicationName}' dropped.`);
  } catch (error) {
    console.log(`Publication '${publicationName}' does not exist or could not be dropped.`);
  }

  // Create publication
  await client.query(`
    CREATE PUBLICATION ${publicationName} FOR ALL TABLES
  `);
  console.log(`Publication '${publicationName}' created.`); 
}