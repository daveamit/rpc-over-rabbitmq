const cbStore = {};

// Topics DONOT support WILDCARD routing keys!!
// Please make sure that the routing keys do not contain
// wild cards like 'entity.configuration.*'
async function newsOn(ch) {
  const { queue } = await ch.assertQueue('', { exclusive: true, autoDelete: true, durable: false });

  ch.consume(queue, (msg) => {
    const payload = JSON.parse((msg.content || '{}').toString());
    if (cbStore[msg.fields.routingKey]) {
      cbStore[msg.fields.routingKey](payload);
    }
  }, { noAck: true });

  // Topics DONOT support WILDCARD routing keys!!
  // Please make sure that the routing keys do not contain
  // wild cards like 'entity.configuration.*' or 'entity.configuration.#'
  return (name, topics, cb) => {
    ch.assertExchange(name, 'topic', { durable: false });
    topics.forEach((topic) => {
      cbStore[topic] = cb.bind(null, topic);
      ch.bindQueue(queue, name, topic);
    });
  };
}


module.exports = newsOn;
