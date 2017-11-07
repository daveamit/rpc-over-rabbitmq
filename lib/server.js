const Ajv = require('ajv');

const ajv = new Ajv(); // options can be passed, e.g. {allErrors: true}

async function server(ch) {
  /*
    Name: name of the service.
    first: if second argument is not provided, first is considered as callback.
    second: if secound argument is provided, first is considered as schema / schema builder.
  */
  return (name, first, second) => {
    const q = name;
    const f = second || first;
    const schema = second ? first : undefined;
    const fetchSchema = schema instanceof Function ? schema : () => schema;
    async function validate(content) {
      const schemaDef = await fetchSchema(content);
      if (schemaDef) {
        const valid = ajv.validate(schemaDef, content);
        if (!valid) {
          throw new Error(ajv.errorsText());
        }
      }
    }

    // if schema is defined.
    if (schema) {
      if (!(schema instanceof Function) && !(schema instanceof Object)) {
        throw new Error(`Invalid schema for ${q}`);
      }
    }

    ch.assertQueue(q, { durable: false, noAck: false });
    ch.prefetch(1);
    ch.consume(q, (msg) => {
      try {
        let content = msg.content; // eslint-disable-line
        if (msg.properties.contentType === 'application/json') {
          content = JSON.parse(content.toString());
        }
        const processResponse = (e, data, opts = {}) => {
          if (!(data instanceof Buffer)) {
            if (e) {
              if (e instanceof Error) {
                    data = { $error: e.toString() }; // eslint-disable-line
              } else {
                    data = { $error: e }; // eslint-disable-line
              }
            }
                data = Buffer.from(JSON.stringify(data)); // eslint-disable-line

                opts.contentType = 'application/json'; // eslint-disable-line
          }
          ch.sendToQueue(
            msg.properties.replyTo,
            data,
            Object.assign({
              correlationId: msg.properties.correlationId,
            }, opts),
          );
          ch.ack(msg);
        };

        validate(content)
          .then(() => {
            f(content, processResponse);
          })
          .catch((e) => {
            processResponse(e);
          });
      } catch (ex) {
        console.log(ex);
      }
    });
  };
}


module.exports = server;
