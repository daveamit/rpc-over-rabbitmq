var amqp = require('amqplib');

async function server(opts) {
    const conn = await amqp.connect(opts);
    const ch = await conn.createChannel();

    return {
        on: (q, f) => {
            ch.assertQueue(q, { durable: false, noAck: false });
            ch.prefetch(1);
            ch.consume(q, (msg) => {
                try {
                    let content = msg.content;
                    if(msg.properties.contentType === 'application/json') {
                        content = JSON.parse(content.toString());
                    }
                    f(content, (e, data, opts = {}) => {

                        if (!(data instanceof Buffer)) {
                            if(data) {
                                data = Buffer.from(JSON.stringify(data));
                            }
                            opts.contentType = 'application/json';
                        }
                        ch.sendToQueue(
                            msg.properties.replyTo,
                            data || Buffer.from( JSON.stringify({ $error: (e || `Unknown error occored while processing ${q}`).toString() })),
                            Object.assign(
                                {
                                    correlationId: msg.properties.correlationId
                                }, opts)
                        );
                        ch.ack(msg);
                    })

                } catch (ex) {
                    console.log(ex);
                }
            });
        }
    }
}


module.exports = server;