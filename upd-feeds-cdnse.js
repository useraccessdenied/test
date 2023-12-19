// CDNSE implementation 
const dgram = require('dgram');
const server = dgram.createSocket('udp4');
// const utf8 = require('utf8');
var onChangeHandler;
// var uuid = require('uuid-random'); // to generate random number of unique id 
// var dateTime = require('node-datetime');
// var utils = require('./utils');
// var utils = require('./util-opt-json');
var utils = require('./util-opt-array');
const config = require('./config');
const Cache = require('map-cache-ttl');
const errorLogger = require(__dirname + '/../../logger').getLogger('error');
const infoLogger = require(__dirname + '/../../logger').getLogger('info');
var receiveCount = 1;

function brodcastFeed() {
    try {
        let scriptCache = new Cache(config.cacheMaxAge, '2s');
        server.on('message', (msg, rinfo) => {
            infoLogger.info(msg.toString('utf-8'));
            receiveCount++;
            // infoLogger.info("CDNSEReceiveCount::" + receiveCount);

            var message = {};
            var msgArray = msg.toString('utf-8').split('^');

            if (msgArray[3] != undefined && scriptCache.has(msgArray[3]) == false) {
                // set in inmemory hash table 
                scriptCache.set(msgArray[3], '1', config.cacheMaxAge);

                // LTP msgArray[20] 
                var script = msgArray[3].split('!');
                if (script.length == 2 && script[0] == '13.1' && msgArray[20] != '' && msgArray[32] != undefined && msgArray[32] != '' && msgArray[32].length > 0) {
                    var message = utils.getExchangeQuote('CDNSE', msgArray);
                    oiValue = msgArray[25];
                    if(oiValue != undefined && oiValue != null && oiValue.toString() != "1"){
                        message[12] = oiValue;
                    }
                    onChangeHandler(msgArray[3], 'stock', message);
                }

                // CDNSE Market Depth: market depth will come at 47 
                if (msgArray.length > 50 && msgArray[50] != undefined && msgArray[50].length > 0) {
                    var scriptMd = msgArray[50].split('!');
                    if (scriptMd.length == 2 && scriptMd[0] == '13.2') {
                        // BSE market 
                        var message = processMarketDepth(msgArray);
                        onChangeHandler(msgArray[50], 'stock', message);
                    }
                }

            } else if (msgArray[3] != undefined && scriptCache.has(msgArray[3]) == true) {
                var counter = scriptCache.get('cdnsecount');
                infoLogger.info("CDNSE counter skipped @ " + counter);
                if (counter != undefined && counter > 0) {
                    var value = counter + 1;
                    scriptCache.set('cdnsecount', value, '8h');
                } else {
                    infoLogger.info("set");
                    scriptCache.set('cdnsecount', 1, '8h');
                }

            }

        });

        server.on('listening', () => {
            const address = server.address();
            infoLogger.info(`CDNSE Server Listening @ ${address.address}:${address.port}`);
        });

        server.bind(config.udpPortCDNSE);

    } catch (error) {
        errorLogger.error("For 'Server-On-message' at Receive Count " + receiveCount + " with msg " + msg + " , CDNSE brodcastFeed() Error: " + error.message);
    }
    server.on('error', (err) => {
        errorLogger.error(`For NSE 'Server-On-error', brodcastFeed() Error: \n${err.stack}`);
        server.close();
    });
}

function processMarketDepth(data, message) {
    try {
        var message = [];
        if (Array.isArray(data) && data.length > 50 && data[50] != '') {
            // message['symbol'] = data[50];
            message.push((data[50] != undefined) ? data[50] : null);
            // message['time'] = data[55]; // recived time at 55 
            dateString = data[55].split(" ")[0].split("/");
            message.push((data[55] != undefined) ? (new Date((dateString[1] + '/' + dateString[0] + '/' + dateString[2] + " " + data[55].split(" ")[1])).getTime()) / 1000 : null);
            // message['depth'] = [];
            var depth = [];
            if (data.length > 58 && data[58] != undefined && !isNaN(data[58])) {
                var NoOfRecs = data[58];
                var index = 58;
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
        errorLogger.error("For Exchange Code 'CDNSE' with data as " + data + " , processMarketDepth() Error: " + error);
        return [];
    }
}

function start(onChange) {
    onChangeHandler = onChange;
    brodcastFeed();
}

exports.start = start;
