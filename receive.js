    if (room.startsWith("1.1")){
        redisClient.hget("close", room, function(err, res) {
            if (err) {
                errorLogger.error("Error: " + err);
            }
            if (res) {
                message[20] = res;
                message[5] = Math.round(((message[2] - message[20]) / message[20]) * 10000) / 100;
            } else {
                errorLogger.error("Undefined value for bse token " + room);
            }
            redisClient.hset("lastKnownStockValue", room, JSON.stringify(message), "EX", 604800);
        });
    }

    if (room.startsWith("4.1")){
        redisClient.hget("close", room, function(err, res) {
            if (err) {
                errorLogger.error("Error: " + err);
            }
            if (res) {
                message[20] = res;
                message[5] = Math.round(((message[2] - message[20]) / message[20]) * 10000) / 100;
            } else {
                errorLogger.error("Undefined value for nse token " + room);
            }
            redisClient.hset("lastKnownStockValue", room, JSON.stringify(message), "EX", 604800);
        });
    }