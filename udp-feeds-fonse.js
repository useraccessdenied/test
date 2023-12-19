const dgram = require('dgram');
const server = dgram.createSocket('udp4');
var onChangeHandler;
var utils = require('./util-opt-array');
const config = require('./config');
const Cache = require('map-cache-ttl');
const errorLogger = require(__dirname + '/../../logger').getLogger('error');
const infoLogger = require(__dirname + '/../../logger').getLogger('info');
var receiveCount = 0;

function brodcastFeed() {
    try {
        let scriptCache = new Cache(config.cacheMaxAge, '2s');
        server.on('message', (msg, rinfo) => {
            // infoLogger.info(msg.toString('utf-8'));
            receiveCount++;
            // infoLogger.info("FONSEReceiveCount::" + receiveCount);

            var msgArray = msg.toString('utf-8').split('^');
            if (msgArray[3] != undefined && scriptCache.has(msgArray[3]) == false && msgArray[32] != undefined && msgArray[32] != '' && msgArray[32].length > 0) {

                // set in inmemory hash table 
                scriptCache.set(msgArray[3], '1', config.cacheMaxAge);

                // LTP msgArray[20] 
                var script = msgArray[3].split('!');
                if (script.length == 2 && script[0] == '4.1' && msgArray[20] != '') {
                    var message = utils.getExchangeQuote('FONSE', msgArray);
                    oiValue = scriptCache.get(msgArray[3]+"oi");
                    if(oiValue != undefined && oiValue != null && oiValue.toString() != "1"){
                        message[12] = oiValue;
                    }
                    onChangeHandler(msgArray[3], 'stock', message);
                }

                // FONSE Market Depth: market depth will come at 47 
                if (msgArray.length > 47 && msgArray[47] != undefined && msgArray[47].length > 0) {
                    var scriptMd = msgArray[47].split('!');
                    if (scriptMd.length == 2 && scriptMd[0] == '4.2') {
                        // BSE market 
                        var message = processMarketDepth(msgArray);
                        onChangeHandler(msgArray[47], 'stock', message);

                    }
                }

            } else if (msgArray[3] != undefined && scriptCache.has(msgArray[3]) == true) {
                var counter = scriptCache.get('fonsecount');
                infoLogger.info("FONSE counter skipped @ " + counter);
                if (counter != undefined && counter > 0) {
                    var value = counter + 1;
                    scriptCache.set('fonsecount', value, '8h');
                } else {
                    scriptCache.set('fonsecount', 1, '8h');
                }

            }

        });

        server.on('listening', () => {
            const address = server.address();
            infoLogger.info(`FONSE Server Listening @ ${address.address}:${address.port}`);
        });

        server.bind(config.udpPortFONSE);

    } catch (error) {
        errorLogger.error("For 'Server-On-message' at Receive Count " + receiveCount + " with msg " + msg + " , FONSE brodcastFeed() Error: " + error.message);
    }

    server.on('error', (err) => {
        errorLogger.error(`For BSE 'Server-On-error', brodcastFeed() Error: \n${err.stack}`);
        server.close();
    });
}

function processMarketDepth(data) {
    try {
        var message = [];
        if (Array.isArray(data) && data.length > 47 && data[47] != '') {
            // var dt = dateTime.create();
            // var todaysDate = dt.format('Y-m-d H:M:S N');
            // message['symbol'] = data[47];
            message.push((data[47] != undefined) ? data[47] : null);
            // message['time'] = data[52]; // time for market depth at 57 position 
            dateString = data[52].split(" ")[0].split("/")
            message.push((data[52] != undefined) ? (new Date((dateString[1] + '/' + dateString[0] + '/' + dateString[2] + " " + data[52].split(" ")[1])).getTime()) / 1000 : null);
            // message['send_on'] = todaysDate;
            // message['depth'] = [];
            var depth = [];
            if (data.length > 55 && data[55] != undefined && !isNaN(data[55])) {
                var NoOfRecs = data[55];
                var index = 55;
                var startIndex = 0;
                for (i = 1; i <= NoOfRecs; i++) {
                    startIndex = index + 1;
                    // var options = {};
                    var options = [];
                    // options['BestBuyRate-1'] = data[startIndex];
                    options.push(parseFloat(data[startIndex]));
                    // options['BestBuyQty-1'] = data[startIndex + 1];
                    options.push(parseFloat(data[startIndex + 1]));
                    // options['BuyNoOfOrders-1'] = data[startIndex + 2];
                    options.push(parseFloat(data[startIndex + 2]));
                    // options['BuyFlag-1'] = data[startIndex + 3];
                    options.push(data[startIndex + 3]);
                    // options['BestSellRate-1'] = data[startIndex + 4];
                    options.push(parseFloat(data[startIndex + 4]));
                    // options['BestSellQty-1'] = data[startIndex + 5];
                    options.push(parseFloat(data[startIndex + 5]));
                    // options['SellNoOfOrders-1'] = data[startIndex + 6];
                    options.push(parseFloat(data[startIndex + 6]));
                    // options['SellFlag-1'] = data[startIndex + 7];
                    options.push(data[startIndex + 7]);
                    depth.push(options);
                    index = startIndex + 7;
                }
            }
            message.push(depth);
        }
        return message;
    } catch (error) {
        errorLogger.error("For Exchange Code 'FONSE' with data as " + data + " , processMarketDepth() Error: " + error);
        return [];
    }
}

function start(onChange) {
    // infoLogger.info("startfunc--" + onChange);
    onChangeHandler = onChange;
    brodcastFeed();
}

exports.start = start;