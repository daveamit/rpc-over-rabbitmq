const amqp = require('amqplib');

const server = require('./lib/server');
const client = require('./lib/client');

module.exports = {
    init,
}

async function init(opts) {
    try {
        const conn = await amqp.connect(opts);
        const [on, call] = await Promise.all([server(conn), client(conn)]);
        module.exports.on = on;
        module.exports.call = call;
        return module.exports;
    } catch (error) {
        console.log(error);
    }
}
