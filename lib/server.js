async function server(conn) {
    const ch = await conn.createChannel();

    return (q, f) => {
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

                            if(e) {
                                if(e instanceof Error) {
                                    data = { $error: e.toString() }
                                } else {
                                    data = { $error: e }
                                }
                            }
                            data = Buffer.from(JSON.stringify(data));
                            
                            opts.contentType = 'application/json';
                        }
                        ch.sendToQueue(
                            msg.properties.replyTo,
                            data,
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


module.exports = server;