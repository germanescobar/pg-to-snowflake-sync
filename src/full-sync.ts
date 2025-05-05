import { Client } from 'pg';
import snowflake from 'snowflake-sdk';
import { Config } from './types';
import { connectToPostgresClient, connectToSnowflake, executeSnowflakeQuery } from './utils';
import { syncSchema } from './schema-sync';
import fs from 'fs';
import fsPromises from 'fs/promises';
import * as csv from 'fast-csv';
import * as path from 'path';

export async function runFullSync(config: Config): Promise<void> {
  let pgClient: Client | null = null;
  let snowflakeConn: snowflake.Connection | null = null;
  
  try {
    console.log('Starting full sync process ...');
    const start = Date.now();
    
    // 1. Connect to services
    pgClient = await connectToPostgresClient(config);
    snowflakeConn = await connectToSnowflake(config);

    // 2. Recreate replication slot
    await recreateReplicationSlot(pgClient, config);
    
    // 3. Create temp directory for CSV exports
    const tempDir = path.join(process.cwd(), 'temp_exports');
    await fsPromises.mkdir(tempDir, { recursive: true });
    
    // 4. Synchronize schema first
    await syncSchema(snowflakeConn, config.sync.excludedTables, config);
    
    // 5. Get list of tables to sync
    const tablesResult = await pgClient.query(`
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = 'public'
      AND table_type = 'BASE TABLE'
    `);
    
    const tables = tablesResult.rows
      .map(row => row.table_name)
      .filter(table => !config.sync.excludedTables.includes(table));
    
    // 6. Create a temporary Snowflake stage for file uploads
    await executeSnowflakeQuery(snowflakeConn, `
      CREATE TEMPORARY STAGE temp_sync_stage
      FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = ',' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1)
    `);
    
    // 7. Process each table
    for (const table of tables) {
      await processTable(pgClient, snowflakeConn, table, tempDir);
    }
    
    // 8. Clean up
    await executeSnowflakeQuery(snowflakeConn, `DROP STAGE IF EXISTS temp_sync_stage`);
    await fsPromises.rm(tempDir, { recursive: true, force: true });
    
    const end = Date.now();
    console.log(`Full resync completed successfully in ${((end - start) / 1000).toFixed(2)} seconds.`);
  } catch (error) {
    console.error('Error during resync:', error);
    throw error;
  } finally {
    if (pgClient) await pgClient.end();
    if (snowflakeConn) {
      snowflakeConn.destroy((err: Error | undefined) => {
        if (err) console.error('Error destroying Snowflake connection:', err);
      });
    }
  }
}

async function recreateReplicationSlot(pgClient: Client, config: Config) {
  // Drop existing replication slot if it exists
  const slotName = config.postgres.replicationSlot;
  try {
    await pgClient.query(`SELECT pg_drop_replication_slot('${slotName}') WHERE EXISTS (
      SELECT 1 FROM pg_replication_slots WHERE slot_name = '${slotName}'
    )`);
    console.log(`Dropped existing replication slot: ${slotName}`);
  } catch (error: any) {
    console.log(`No existing replication slot found or could not drop: ${error.message}`);
  }
  
  // Create a new replication slot at the current position
  await pgClient.query(`
    SELECT pg_create_logical_replication_slot(
      '${slotName}', 
      'pgoutput'
    )
  `);
  console.log(`Recreated the replication slot: ${slotName}`);
}

async function processTable(pgClient: Client, snowflakeConn: snowflake.Connection, table: string, tempDir: string) {
  console.log(`Processing table: ${table}`);
      
  // 7a. Get column information
  const columnResult = await pgClient.query(`
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'public'
    AND table_name = $1
    ORDER BY ordinal_position
  `, [table]);
  
  const columns = columnResult.rows.map(r => r.column_name);
  
  // Create a map of timestamp columns for special handling
  const timestampColumns = new Set<string>();
  const dateColumns = new Set<string>();
  columnResult.rows.forEach(col => {
    if (col.data_type.includes('timestamp')) {
      timestampColumns.add(col.column_name);
    } else if (col.data_type === 'date') {
      dateColumns.add(col.column_name);
    } else if (col.data_type === 'time') {
      timestampColumns.add(col.column_name);
    }
  });
  
  console.log(`Table ${table} - Date columns:`, Array.from(dateColumns));
  console.log(`Table ${table} - Timestamp columns:`, Array.from(timestampColumns));
  
  // 7b. Export data to CSV
  const csvFilePath = path.join(tempDir, `${table}.csv`);
  const writeStream = fs.createWriteStream(csvFilePath);
  const csvStream = csv.format({ 
    headers: true,
    quote: '"',
    escape: '"',
    quoteColumns: true,  // Quote all fields for maximum compatibility
    quoteHeaders: true,
    rowDelimiter: '\n',
    writeHeaders: true
  });
  csvStream.pipe(writeStream);
  
  // Export in batches to handle large tables
  const batchSize = 10000;
  let offset = 0;
  let hasMoreRows = true;
  let totalRowsExported = 0;
  
  while (hasMoreRows) {
    const result = await pgClient.query(`
      SELECT * FROM "${table}" 
      ORDER BY 1 
      LIMIT ${batchSize} OFFSET ${offset}
    `);
    
    if (result.rows.length === 0) {
      hasMoreRows = false;
      continue;
    }
    
    // Format data properly before writing to CSV
    const formattedRows = result.rows.map(row => {
      const formattedRow: Record<string, any> = {};
      
      // Process each column with appropriate formatting
      for (const key in row) {
        const value = row[key];
        const isTimestampColumn = timestampColumns.has(key);
        const isDateColumn = dateColumns.has(key);
        
        if (value === null || value === undefined || value === '') {
          // Explicitly use 'NULL' string for null values in CSV
          formattedRow[key] = 'NULL';
        } else if (value instanceof Date) {
          // Format as ISO string without timezone info for Snowflake compatibility
          // Also check for invalid dates (e.g., 0000-00-00)
          if (isNaN(value.getTime())) {
            formattedRow[key] = 'NULL';
          } else if (isDateColumn) {
            // For date columns, only include the date part (YYYY-MM-DD)
            formattedRow[key] = value.toISOString().split('T')[0];
          } else {
            // For timestamp columns, include date and time
            formattedRow[key] = value.toISOString().replace('T', ' ').replace('Z', '');
          }
        } else if ((isTimestampColumn || isDateColumn) && typeof value === 'string' && value.trim() === '') {
          // Handle string timestamp/date fields that are empty strings
          formattedRow[key] = 'NULL';
        } else if (isDateColumn && typeof value === 'string' && value.includes(' ')) {
          // For date columns that contain timestamps as strings, extract just the date part
          try {
            const dateOnly = value.split(' ')[0];
            formattedRow[key] = dateOnly;
          } catch (err) {
            console.warn(`Failed to parse date string in ${table}.${key}, keeping original: ${value}`);
            formattedRow[key] = value;
          }
        } else if (typeof value === 'object') {
          // Convert objects (like JSON) to strings
          try {
            formattedRow[key] = JSON.stringify(value);
          } catch (err) {
            console.warn(`Failed to stringify object in ${table}.${key}, setting to null:`, err);
            formattedRow[key] = 'NULL';
          }
        } else {
          // Handle other primitive types
          formattedRow[key] = value;
        }
      }
      
      return formattedRow;
    });
    
    // Write formatted batch to CSV
    for (const row of formattedRows) {
      try {
        csvStream.write(row);
      } catch (err) {
        console.error(`Error writing row to CSV for table ${table}:`, err);
      }
    }
    
    totalRowsExported += result.rows.length;
    offset += result.rows.length;
    console.log(`Exported ${totalRowsExported} rows for table ${table}`);
  }
  
  // Close the CSV stream properly
  csvStream.end();
  console.log(`Waiting for CSV file to finish writing: ${csvFilePath}`);
  await new Promise<void>((resolve, reject) => {
    writeStream.on('finish', () => {
      console.log(`CSV file completed for ${table}`);
      resolve();
    });
    writeStream.on('error', (err) => {
      console.error(`Error writing CSV file for ${table}:`, err);
      reject(err);
    });
  });
  
  // Verify the CSV file exists and has content
  try {
    const stats = await fsPromises.stat(csvFilePath);
    console.log(`CSV file size for ${table}: ${stats.size} bytes`);
    if (stats.size === 0) {
      console.error(`CSV file for ${table} is empty!`);
      return;
    }
  } catch (err) {
    console.error(`Error checking CSV file for ${table}:`, err);
    return;
  }
  
  // 7c. Upload CSV to Snowflake stage
  await uploadFileToSnowflake(snowflakeConn, csvFilePath, `temp_sync_stage/${table}.csv`);
  
  // 7d. Load data into Snowflake table (with truncate)
  await executeSnowflakeQuery(snowflakeConn, `TRUNCATE TABLE "${table.toUpperCase()}"`);
  
  try {
    // Run COPY command with validation mode first to check for errors
    const validationResult = await executeSnowflakeQuery(snowflakeConn, `
      COPY INTO "${table.toUpperCase()}"
      FROM @temp_sync_stage/${table}.csv
      FILE_FORMAT = (
        TYPE = 'CSV' 
        FIELD_DELIMITER = ',' 
        FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
        SKIP_HEADER = 1
        EMPTY_FIELD_AS_NULL = TRUE
        NULL_IF = ('NULL', 'null', '')
        DATE_FORMAT = 'AUTO'
        TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'
        BINARY_FORMAT = 'HEX'
      )
      VALIDATION_MODE = RETURN_ERRORS
    `);
    
    console.log(`Validation results for ${table}:`, JSON.stringify(validationResult, null, 2));
    
    // Now run the actual copy command
    const copyResult = await executeSnowflakeQuery(snowflakeConn, `
      COPY INTO "${table.toUpperCase()}"
      FROM @temp_sync_stage/${table}.csv
      FILE_FORMAT = (
        TYPE = 'CSV' 
        FIELD_DELIMITER = ',' 
        FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
        SKIP_HEADER = 1
        EMPTY_FIELD_AS_NULL = TRUE
        NULL_IF = ('NULL', 'null', '')
        DATE_FORMAT = 'AUTO'
        TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'
        BINARY_FORMAT = 'HEX'
      )
      ON_ERROR = 'ABORT_STATEMENT'
    `);
    
    console.log(`Copy results for ${table}:`, JSON.stringify(copyResult, null, 2));
  } catch (error) {
    console.error(`Error loading data for table ${table}:`, error);
    
    // Check if there are errors in the staging area
    try {
      const errorRows = await executeSnowflakeQuery(snowflakeConn, `
        SELECT * FROM TABLE(VALIDATE(${table.toUpperCase()}, 
          JOB_ID => '_LAST'))
      `);
      console.error(`Validation errors for ${table}:`, JSON.stringify(errorRows, null, 2));
    } catch (validationError) {
      console.error(`Error checking validation for ${table}:`, validationError);
    }
  }
  
  console.log(`Completed loading data for table: ${table}`);
}

// Helper function to upload a file to Snowflake stage
async function uploadFileToSnowflake(
  snowflakeConn: snowflake.Connection, 
  filePath: string, 
  stagePath: string
): Promise<void> {
  return new Promise((resolve, reject) => {
    console.log(`Starting upload of ${filePath} to Snowflake stage @${stagePath}...`);
    
    // First check if the file exists and has size
    try {
      const stats = fs.statSync(filePath);
      console.log(`Local file size: ${stats.size} bytes`);
      if (stats.size === 0) {
        console.error('File is empty, not uploading');
        reject(new Error('File is empty'));
        return;
      }
    } catch (err) {
      console.error(`Error checking file: ${err}`);
      reject(err);
      return;
    }
    
    // Execute the PUT command with detailed logging
    snowflakeConn.execute({
      sqlText: `PUT file://${filePath} @${stagePath} AUTO_COMPRESS=TRUE OVERWRITE=TRUE`,
      complete: (err: any, stmt: any, rows: any) => {
        if (err) {
          console.error(`Error uploading file to Snowflake: ${err}`);
          reject(err);
        } else {
          console.log(`Upload complete. Result: ${JSON.stringify(rows, null, 2)}`);
          
          // Verify the file is in the stage
          snowflakeConn.execute({
            sqlText: `LIST @${stagePath.split('/')[0]}`,
            complete: (listErr: any, listStmt: any, listRows: any) => {
              if (listErr) {
                console.warn(`Warning: Could not verify upload: ${listErr}`);
                resolve(); // Continue despite verification error
              } else {
                console.log(`Files in stage: ${JSON.stringify(listRows, null, 2)}`);
                resolve();
              }
            }
          });
        }
      }
    });
  });
}