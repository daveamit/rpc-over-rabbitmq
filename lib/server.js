const Ajv = require('ajv');
const jwt = require('jsonwebtoken');
const uuid = require('uuid/v4');

const ajv = new Ajv(); // options can be passed, e.g. {allErrors: true}
const secret = 'b@d-a$$';

const permissionsStore = {};

// This makes sure that the roles provided in the decodedToken
// actually have permission to execute given rpc
// otherwise it would throw access denied error.
const defaultActl = async (call, { tenant }, rpcname) => {
  if (rpcname === 'entity.findOne') {
    // Returning to avoid Chicken-Egg problem
    return;
  }

  if (!permissionsStore[tenant]) {
    permissionsStore[tenant] = {};
  }
  if (!permissionsStore[tenant][rpcname]) {
    permissionsStore[tenant][rpcname] = await call('entity.findOne', { entity: { type: 'permission', id: rpcname } });
  }
};
const defaultTokenDecode = token => jwt.verify(token, secret);

async function server(ch) {
  /*
    Name: name of the service.
    first: if second argument is not provided, first is considered as callback.
    second: if secound argument is provided, first is considered as schema / schema builder.
  */
  return refToCall => (name, first, second) => {
    const q = name;
    const f = second || first;
    const schema = second ? first.schema : undefined;
    const actl = (second ? first.actl : undefined) || defaultActl;
    const tokenDecode = (second ? first.tokenDecode : undefined) || defaultTokenDecode;

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
    ch.prefetch((process.env.PREFETCH && parseInt(process.env.PREFETCH)) || 1);
    ch.consume(q, (msg) => {
      try {
        let content = msg.content; // eslint-disable-line
        if (msg.properties.contentType === 'application/json') {
          content = JSON.parse(content.toString());
          if (!content.$requestId) {
            content.$requestId = uuid();
          }
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

        // Insert decoded token into content.
        content.decodedToken = content.token ? tokenDecode(content.token) : undefined;
        const call = (rpcName, args, options = {}) => refToCall(rpcName, args, { token: content.token, $requestId: content.$requestId, ...options });

        actl(call, content.decodedToken, name)
          .then(() => validate(content))
          .then(() => {
            f({ call, content }, processResponse);
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
