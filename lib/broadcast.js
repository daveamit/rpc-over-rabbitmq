async function broadcast(ch) {
  return (name, message) => {
    const [ex, ...restRoutingKey] = name.split('.');
    const routingKey = restRoutingKey.join('.');

    ch.assertExchange(ex, 'topic', { durable: false });
    ch.publish(ex, routingKey, Buffer.from(JSON.stringify(message)));
  };
}


module.exports = broadcast;
