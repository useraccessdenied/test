var ioRedis = require("ioredis");
config = require("./config");

let pattern = "4.2";

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
  console.log("Last-Known Stock Value in Redis " + error);
});

redisClient
  .hgetall("lastKnownStockValue")
  .then((res) => {
    let count = 0;
    Object.keys(res).forEach((e) => {
      if (e.startsWith(pattern)) {
        count += 1;
        redisClient.hdel("lastKnownStockValue", e);
      }
      // redisClient.hdel("lastKnownStockValue", e);
      // count += 1;
    });
    console.log("Total deleted keys: " + count);
    console.log("Exit with Ctrl-C");
  })
  .catch((err) => {
    console.error("Error getting stocks!");
  });
