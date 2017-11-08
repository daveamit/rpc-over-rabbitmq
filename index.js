const amqp = require('amqplib');

const server = require('./lib/server');
const client = require('./lib/client');
const broadcastMod = require('./lib/broadcast');
const newsOnMod = require('./lib/newsOn');

async function init(opts) {
  try {
    const conn = await amqp.connect(opts);
    const ch = await conn.createChannel();

    const [on, call, broadcast, newsOn] = await Promise.all([server(ch), client(ch), broadcastMod(ch), newsOnMod(ch)]);
    module.exports.on = on;
    module.exports.call = call;
    module.exports.broadcast = broadcast;
    module.exports.newsOn = newsOn;
    return module.exports;
  } catch (error) {
    throw error;
  }
}
module.exports = {
  init,
};
