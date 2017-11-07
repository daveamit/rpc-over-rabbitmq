const amqp = require('amqplib');

const server = require('./lib/server');
const client = require('./lib/client');

async function init(opts) {
  try {
    const conn = await amqp.connect(opts);
    const ch = await conn.createChannel();
    
    const [on, call] = await Promise.all([server(ch), client(ch)]);
    module.exports.on = on;
    module.exports.call = call;
    return module.exports;
  } catch (error) {
    throw error;
  }
}
module.exports = {
  init,
};
