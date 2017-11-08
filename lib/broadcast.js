async function broadcast(ch) {
  return async (name, message) => {
    const [ex, ...restRoutingKey] = name.split('.');
    const routingKey = restRoutingKey.join('.');

    await ch.assertExchange(ex, 'topic', { durable: false });
    ch.publish(ex, routingKey, Buffer.from(JSON.stringify(message)));
  };
}


module.exports = broadcast;
