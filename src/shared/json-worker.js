'use strict';

const { parentPort } = require('worker_threads');

parentPort.on('message', (data) => {
    try {
        const jsonString = JSON.stringify(data.body);
        parentPort.postMessage({ id: data.id, result: jsonString });
    } catch (error) {
        parentPort.postMessage({ id: data.id, error: error.message });
    }
});
