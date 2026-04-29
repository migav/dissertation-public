import sqlite3 from 'sqlite3';
import path from 'path';
import { fileURLToPath } from 'url';

export default class DB {
    constructor() {
        const __dirname = path.dirname(fileURLToPath(import.meta.url));
        const dbPath = path.join(__dirname, '../data/funding_rate.db');
        this.db = new sqlite3.Database(dbPath);
    }

    async setupTable() {
        return new Promise((resolve, reject) => {
            this.db.run(`CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY,
                exchange TEXT DEFAULT 'bybit',
                pair TEXT,
                timestamp INTEGER,
                spotPrice REAL,
                futuresPrice REAL,
                indexPrice REAL,
                premiumIndex REAL,
                estimatedFundingRate REAL,
                realisedFundingRate REAL,
                UNIQUE(exchange, pair, timestamp)
            )`, (err) => {
                if (err) reject(err);
                resolve();
            });
        });
    }

    async migrateAddExchangeColumn() {
        return new Promise((resolve, reject) => {
            // Check if exchange column exists
            this.db.all("PRAGMA table_info(snapshots)", (err, columns) => {
                if (err) return reject(err);
                
                const hasExchange = columns.some(col => col.name === 'exchange');
                if (hasExchange) {
                    console.log('Exchange column already exists');
                    return resolve();
                }

                // Add exchange column with default 'bybit' for existing data
                this.db.run(`ALTER TABLE snapshots ADD COLUMN exchange TEXT DEFAULT 'bybit'`, (alterErr) => {
                    if (alterErr) return reject(alterErr);
                    console.log('Added exchange column to snapshots table');
                    resolve();
                });
            });
        });
    }



    getSnapshots(pair, startDate, endDate, exchange = 'bybit') {
        console.log(exchange, pair, startDate, endDate);
        return new Promise((resolve, reject) => {
            this.db.all(
                `SELECT * FROM snapshots WHERE exchange = ? AND pair = ? AND timestamp >= ? AND timestamp <= ?`,
                [exchange, pair, startDate, endDate],
                (err, rows) => {
                    if (err) {
                        console.log(err);
                        return reject(err);
                    }
                    resolve(rows);
                }
            );
        });
    }

    insertSnapshots(pair, snapshots, exchange = 'bybit') {
        return new Promise((resolve, reject) => {
            this.db.serialize(() => {
                this.db.run('BEGIN TRANSACTION');
                const stmt = this.db.prepare(
                    `INSERT OR IGNORE INTO snapshots (exchange, pair, timestamp, spotPrice, futuresPrice, indexPrice, premiumIndex, estimatedFundingRate, realisedFundingRate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
                );
                for (let i = 0; i < snapshots.length; i++) {
                    stmt.run(
                        exchange,
                        pair,
                        snapshots[i][0],
                        snapshots[i][1],
                        snapshots[i][2],
                        snapshots[i][3],
                        snapshots[i][4],
                        snapshots[i][5],
                        snapshots[i][6]
                    );
                }
                stmt.finalize((err) => {
                    if (err) return reject(err);
                    this.db.run('COMMIT', (commitErr) => {
                        if (commitErr) return reject(commitErr);
                        resolve();
                    });
                });
            });
        });
    }

    getAllSnapshots() {
        return new Promise((resolve, reject) => {
            this.db.all(`SELECT * FROM snapshots`, (err, rows) => {
                if (err) {
                    console.log(err);
                    return reject(err);
                }
                resolve(rows);
            });
        });
    }

}