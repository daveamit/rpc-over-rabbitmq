const uuid = require('uuid/v4');

const store = {};

async function client(ch) {
  // const q = await ch.assertQueue('', { exclusive: true, autoDelete: true, durable: false });

  // Direct Reply to mprove performance
  // https://www.rabbitmq.com/direct-reply-to.html
  const q = { queue: 'amq.rabbitmq.reply-to' };

  ch.consume(q.queue, (msg) => {
    const cb = store[msg.properties.correlationId];
    const content = msg.properties.contentType === 'application/json' ? JSON.parse(msg.content.toString()) : msg.content;

    // if there is any error, it will be presented in $error prop.
    const err = content.$error ? content.$error : null;

    if (cb) {
      if (!cb(err, err ? null : content, msg)) {
        delete store[msg.properties.correlationId];
      }
    }
  }, {
    noAck: true,
  });

  return (name, args, options = { contentType: 'application/json' }) => new Promise((resolve, reject) => {
    const corr = uuid();
    let content = args || {};
    store[corr] = (e, data) => {
      if (e) {
        reject(e);
      } else {
        resolve(data);
      }
    };
    if (!(args instanceof Buffer)) {
      if (options.token) {
        content.token = options.token;
      }
      content = Buffer.from(JSON.stringify(content));
      options.contentType = 'application/json'; // eslint-disable-line
    }

    ch.sendToQueue(
      name,
      content,
      Object.assign({
        correlationId: corr,
        replyTo: q.queue,
      }, options),
    );
  });
}


module.exports = client;
