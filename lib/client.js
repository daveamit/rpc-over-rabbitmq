const amqp = require('amqplib');
const uuid = require('uuid/v4');

const store = {};

async function client(opts) {
  const conn = await amqp.connect(opts);
  const ch = await conn.createChannel();

  const q = await ch.assertQueue('', { exclusive: true, autoDelete: true, durable: false });
  ch.prefetch(1);

  ch.consume(q.queue, (msg) => {
    const cb = store[msg.properties.correlationId];
    const content = msg.properties.contentType === 'application/json' ? msg.content : msg.content;

    if (cb) {
      if (!cb(null, content, msg)) {
        delete store[msg.properties.correlationId];
      }
    }
  }, {
    noAck: true,
  });

  return {
    call: (name, args, f, options = { contentType: 'application/json' }) => {
      const corr = uuid();
      let content = args;
      store[corr] = f;
      if (!(args instanceof Buffer)) {
        content = Buffer.from(JSON.stringify(content));
        options.contentType = 'application/json'; // eslint-disable-line
      }

      ch.sendToQueue(
        'fibonacci',
        content,
        Object.assign({
          correlationId: corr,
          replyTo: q.queue,
        }, options)
      );
    },
  };
}


module.exports = client;
