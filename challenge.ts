/**
 * The entry point function. This will download the given dump file, extract/decompress it,
 * parse the CSVs within, and add the data to a SQLite database.
 * This is the core function you'll need to edit, though you're encouraged to make helper
 * functions!
 * 
 */
import fs from 'fs';
import https from 'https';
import zlib from 'zlib';
import tar from 'tar';
import csv from 'fast-csv';
import knex, { Knex } from 'knex';

// Function to download a file from a given URL and save it to a specified path
const downloadFile = (url: string, filePath: string): Promise<void> => {
  return new Promise((resolve, reject) => {
    const fileStream = fs.createWriteStream(filePath);
    // Issue a GET request to the provided URL
    const request = https.get(url, (response) => {
      // Pipe the incoming data into a file stream to write to disk
      response.pipe(fileStream);
      // Resolve the promise once the file has been fully written
      fileStream.on('finish', resolve);
      // Reject the promise if there's an error writing to the file
      fileStream.on('error', reject);
    });
    // Reject the promise if there's an error with the HTTP request
    request.on('error', reject);
  });
};

// Function to extract the contents of a .tar.gz file
const extractTarGz = (filePath: string, extractPath: string): Promise<void> => {
  return new Promise((resolve, reject) => {
    // Create a read stream for the provided file path
    fs.createReadStream(filePath)
      // Pipe the file through a gunzip stream to decompress
      .pipe(zlib.createGunzip())
      // Extract the decompressed tarball to the specified directory
      .pipe(tar.extract({ cwd: extractPath }))
      // Resolve the promise once the extraction is complete
      .on('finish', resolve)
      // Reject the promise if there's an error during extraction
      .on('error', reject);
  });
};

// Function to initialize and return a new SQLite database connection
const createDatabase = (): Knex => {
  return knex({
      client: 'sqlite3',
      connection: {
          filename: 'out/database.sqlite',
      },
      // Define the connection pool size for the database
      pool: {
          min: 2,
          max: 10
      },
      useNullAsDefault: true,
  });
};

// Function to set up the initial tables (organizations and customers) in the database
async function setupTables(db: Knex): Promise<void> {
  // Check and create the 'organizations' table if it doesn't exist
  if (!await db.schema.hasTable('organizations')) {
      await db.schema.createTable('organizations', table => {
        table.integer('id').primary();
        table.integer('index');
        table.string('organization id');
        table.string('name');
        table.string('country');
        table.integer('founded');
        table.string('website');
        table.string('description');
        table.string('number of employees');
        table.string('industry');
      });
  }

  // Check and create the 'customers' table if it doesn't exist
  if (!await db.schema.hasTable('customers')) {
      await db.schema.createTable('customers', table => {
        table.integer('id').primary();
        table.integer('index');
        table.string('customer id');
        table.string('company');
        table.string('first name');
        table.string('last name');
        table.string('city');
        table.string('phone 1');
        table.string('phone 2');
        table.string('email');
        table.string('address');
        table.string('country');
        table.string('website');
        table.string('subscription');
        table.string('subscription date');
      });
  }
}

// Function to perform batch insertion of data into the database
const insertBatch = async (db: Knex, table: string, batch: any[], batchSize: number): Promise<void> => {
  await db.batchInsert(table, batch, batchSize);
  // Clear the batch array after insertion
  batch.length = 0;
};

// Function to process CSV data and insert it into the database
const processCSV = async (csvPath: string, db: Knex, table: string, batchSize: number): Promise<void> => {
  const batch: Array<Record<string, any>> = [];
  const readStream = fs.createReadStream(csvPath).pipe(csv.parse({ headers: true }));

  return new Promise((resolve, reject) => {
      // For each row in the CSV
      readStream
          .on('data', async (row) => {
              batch.push(row);
              // If batch size reaches the threshold, pause stream, insert batch, then resume stream
              if (batch.length >= batchSize) {
                  readStream.pause();
                  await insertBatch(db, table, batch, batchSize);
                  readStream.resume();
              }
          })
          .on('end', async () => {
              // Insert any remaining rows from the batch
              if (batch.length > 0) {
                  await insertBatch(db, table, batch, batchSize);
              }
              resolve();
          })
          .on('error', reject);
  });
};


export async function processDataDump(): Promise<void> {
  const downloadUrl = 'https://fiber-challenges.s3.amazonaws.com/dump.tar.gz';
  const downloadPath = 'tmp/dump.tar.gz';
  const extractPath = 'tmp/extracted';
  const csvPath1 = `${extractPath}/dump/organizations.csv`;
  const csvPath2 = `${extractPath}/dump/customers.csv`;
  const batchSize = 100;

  await downloadFile(downloadUrl, downloadPath);
  await extractTarGz(downloadPath, extractPath);

  const db = createDatabase();
  await setupTables(db)

  await processCSV(csvPath1, db, 'organizations', batchSize);
  await processCSV(csvPath2, db, 'customers', batchSize);

  await db.destroy();
};