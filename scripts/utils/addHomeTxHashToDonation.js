const mongoose = require('mongoose');
require('mongoose-long')(mongoose);
require('../../src/models/mongoose-bn')(mongoose);
const Web3 = require('web3');
const ForeignGivethBridgeArtifact = require('giveth-bridge/build/ForeignGivethBridge.json');
const eventDecodersFromArtifact = require('../../src/blockchain/lib/eventDecodersFromArtifact');
const topicsFromArtifacts = require('../../src/blockchain/lib/topicsFromArtifacts');
const toWrapper = require('../../src/utils/to');

const configFileName = 'default'; // default or beta

// eslint-disable-next-line import/no-dynamic-require
const config = require(`../../config/${configFileName}.json`);

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

const Donations = require('../../src/models/donations.model').createModel(app);

const { verifiedTransfers } = require('./eventProcessingHelper.json');

const terminateScript = (message = '', code = 0) =>
  process.stdout.write(`Exit message: ${message}\n`, () => process.exit(code));

// Instantiate Web3 module
// @params {string} url blockchain node url address
const instantiateWeb3 = url => {
  const provider =
    url && url.startsWith('ws')
      ? new Web3.providers.WebsocketProvider(url, {
          clientConfig: {
            maxReceivedFrameSize: 100000000,
            maxReceivedMessageSize: 100000000,
          },
        })
      : url;
  return new Web3(provider);
};

const { nodeUrl } = config.blockchain;

const foreignWeb3 = instantiateWeb3(nodeUrl);

const getHomeTxHash = async txHash => {
  const decoders = eventDecodersFromArtifact(ForeignGivethBridgeArtifact);

  const [err, receipt] = await toWrapper(foreignWeb3.eth.getTransactionReceipt(txHash));

  if (err || !receipt) {
    console.error('Error fetching transaction, or no tx receipt found ->', err, receipt);
    return undefined;
  }

  const topics = topicsFromArtifacts([ForeignGivethBridgeArtifact], ['Deposit']);

  // get logs we're interested in.
  const logs = receipt.logs.filter(log => topics.some(t => t.hash === log.topics[0]));

  if (logs.length === 0) return undefined;

  const log = logs[0];

  const topic = topics.find(t => t.hash === log.topics[0]);
  const event = decoders[topic.name](log);

  return event.returnValues.homeTx;
};

const mongoUrl = config.mongodb;
console.log('url:', mongoUrl);
mongoose.connect(mongoUrl);
const db = mongoose.connection;

db.on('error', err => console.error('Could not connect to Mongo', err));

db.once('open', () => {
  console.log('Connected to Mongo');

  // Uniq set of added donations transaction hash
  const targetTxHashes = Array.from(new Set(verifiedTransfers.map(t => t.txHash)));

  const map = new Map();

  Donations.find(
    {
      txHash: { $in: targetTxHashes },
      homeTxHash: { $exists: false },
    },
    'txHash',
  )
    .cursor()
    .eachAsync(async d => {
      const { _id, txHash } = d;
      if (map.has(txHash)) {
        const homeTxHash = map.get(txHash);
        await Donations.update({ _id }, { homeTxHash }).exec();
        return;
      }

      console.log('----------------------------');
      console.log('Finding home transaction hash for txHash:', txHash);
      const homeTxHash = await getHomeTxHash(txHash);

      if (homeTxHash === undefined) {
        console.log(`Couldn't find home transaction hash for txHash: ${txHash}`);
        return;
      }

      console.log('homeTxHash:', homeTxHash);
      map.set(txHash, homeTxHash);
      await Donations.update({ _id }, { homeTxHash }).exec();
    })
    .then(() => process.exit(0));
});
