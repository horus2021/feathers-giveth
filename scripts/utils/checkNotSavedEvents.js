const mongoose = require('mongoose');
const fs = require('fs');
require('mongoose-long')(mongoose);
require('../../src/models/mongoose-bn')(mongoose);

const configFileName = 'beta'; // default or beta

// eslint-disable-next-line import/no-dynamic-require
const config = require(`../../config/${configFileName}.json`);
const { liquidPledgingAddress, requiredConfirmations } = config.blockchain;

const appFactory = () => {
  const data = {};
  return {
    get(key) {
      return data[key];
    },
    set(key, val) {
      data[key] = val;
    },
  };
};

const app = appFactory();
app.set('mongooseClient', mongoose);

const Events = require('../../src/models/events.model').createModel(app);

const { EventStatus } = require('../../src/models/events.model');

const terminateScript = (message = '', code = 0) =>
  process.stdout.write(`Exit message: ${message}`, () => process.exit(code));

const savedEventPath = `./liquidPledgingEvents_${configFileName}.json`;

const savedEvents = JSON.parse(fs.readFileSync(savedEventPath));
const dbEventsSet = new Set();

const getEventKey = ({ transactionHash, logIndex }) => `${transactionHash}-${logIndex}`;

const main = async () => {
  await Events.find({
    address: liquidPledgingAddress,
  })
    .select(['transactionHash', 'logIndex'])
    .cursor()
    .eachAsync(e => {
      dbEventsSet.add(getEventKey(e));
    });

  // eslint-disable-next-line no-restricted-syntax
  for (const event of savedEvents) {
    if (!dbEventsSet.has(getEventKey(event))) {
      console.log(`This event is not saved in db!\n${JSON.stringify(event, null, 2)}`);
      if (!event || !event.event || !event.signature || !event.returnValues || !event.raw) {
        console.error('Attempted to add undefined event or event with undefined values');
      } else {
        console.log('Creating...');
        // eslint-disable-next-line no-await-in-loop
        await Events.create({
          ...event,
          confirmations: requiredConfirmations,
          status: EventStatus.WAITING,
        });
      }
    }
  }
};

const mongoUrl = config.mongodb;
console.log('url:', mongoUrl);
mongoose.connect(mongoUrl);

const db = mongoose.connection;
db.on('error', err => console.error('Could not connect to Mongo', err));
db.once('open', () => {
  console.log('Connected to Mongo');

  main().then(() => terminateScript('Finished', 0));
});
