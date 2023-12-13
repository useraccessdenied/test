var ioRedis = require("ioredis");
config = require("./config");
const fs = require("fs");

var redisClient;

var redisClient = null;
if (config.isRedisCluster) {
  redisClient = new ioRedis.Cluster([
    {
      host: config.redisHostUrl,
      port: config.redisHostPort,
    },
  ]);
} else {
  redisClient = new ioRedis({
    host: config.redisHostUrl,
    port: config.redisHostPort,
  });
}

redisClient.on("error", function (error) {
  console.log("Store Last-Known Stock Value in Redis " + error);
});

redisClient
  .keys("*!*")
  .then((res) => {
    console.log("Total keys: " + res.length);
    fs.writeFileSync("keys.txt", res.join("\n"));
  })
  .catch((err) => {
    console.error("Error getting keys!");
  });

redisClient
  .hgetall("lastKnownStockValue")
  .then((res) => {
    console.log("Total stocks: " + Object.keys(res).length);
    fs.writeFileSync("stocks.txt", Object.values(res).join("\n"));
  })
  .catch((err) => {
    console.error("Error getting stocks!");
  });
