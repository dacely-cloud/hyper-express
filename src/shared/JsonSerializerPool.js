'use strict';

const { Worker } = require('worker_threads');
const path = require('path');
const os = require('os');

/**
 * Worker pool for async JSON serialization to avoid blocking the event loop
 */
class JsonSerializerPool {
    #workers = [];
    #pendingRequests = new Map();
    #nextId = 0;
    #nextWorkerIndex = 0;
    #initialized = false;

    /**
     * Initialize the worker pool
     * @param {number} poolSize - Number of workers (default: number of CPUs)
     */
    initialize(poolSize = Math.max(2, Math.min(os.cpus().length, 4))) {
        if (this.#initialized) return;
        this.#initialized = true;

        const workerPath = path.join(__dirname, 'json-worker.js');

        for (let i = 0; i < poolSize; i++) {
            const worker = new Worker(workerPath);

            worker.on('message', (msg) => {
                const pending = this.#pendingRequests.get(msg.id);
                if (pending) {
                    this.#pendingRequests.delete(msg.id);
                    if (msg.error) {
                        pending.reject(new Error(msg.error));
                    } else {
                        pending.resolve(msg.result);
                    }
                }
            });

            worker.on('error', (error) => {
                console.error('JSON worker error:', error);
            });

            this.#workers.push(worker);
        }
    }

    /**
     * Serialize an object to JSON string asynchronously
     * @param {any} body - Object to serialize
     * @returns {Promise<string>} - JSON string
     */
    stringify(body) {
        if (!this.#initialized) {
            this.initialize();
        }

        return new Promise((resolve, reject) => {
            const id = this.#nextId++;
            this.#pendingRequests.set(id, { resolve, reject });

            // Round-robin worker selection
            const worker = this.#workers[this.#nextWorkerIndex];
            this.#nextWorkerIndex = (this.#nextWorkerIndex + 1) % this.#workers.length;

            worker.postMessage({ id, body });
        });
    }

    /**
     * Terminate all workers
     */
    terminate() {
        for (const worker of this.#workers) {
            worker.terminate();
        }
        this.#workers = [];
        this.#initialized = false;
    }
}

// Singleton instance
const pool = new JsonSerializerPool();

module.exports = pool;
