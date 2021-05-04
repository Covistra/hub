const { createManager, createOrchestrator, createWorker } = require("conveyor-mq");
const { Observable } = require("rxjs");
const Redis = require("ioredis");
const { DateTime } = require("luxon");
const shortid = require("shortid-36");

module.exports = function hubFactory(opts = {}) {
  const redisConfig = { host: opts.host || process.env.REDIS_HOST || "localhost", port: opts.port || process.env.REDIS_PORT || 6379 };

  return {
    Queue: function (name) {
      const manager = createManager({
        queue: name,
        redisConfig,
      });

      const orchestrator = createOrchestrator({
        queue: name,
        redisConfig,
      });

      return {
        orchestrator,
        manager,
        async close() {
          await manager.quit();
          await orchestrator.quit();
        },
        async publish(data, opts = {}) {
          const id = shortid.generate();
          const { task } = await manager.enqueueTask({ id, data, ...opts });
          const result = await manager.onTaskComplete(task.id);
          if (result.error) {
            throw result.error;
          } else {
            return result.result;
          }
        },
        worker(fn, opts = {}) {
          const workerOpts = {
            queue: name,
            concurrency: opts.concurrency || 1,
            expiresAt:
              opts.expiresAt ||
              DateTime.local()
                .plus({ seconds: opts.timeout || 30 })
                .toJSDate(),
            redisConfig,
            autoStart: true,
            ...opts,
          };
          if (fn) {
            createWorker({ ...workerOpts, handler: fn });
          } else {
            return new Observable(function workerObserver(obs) {
              createWorker({
                ...workerOpts,
                handler: async function (context) {
                  obs.next(context);
                },
              });
            });
          }
        },
      };
    },
    Channel: function (key, opts = {}) {
      console.log("creating channel");

      const pub = new Redis(opts.url);
      const sub = new Redis();

      return {
        publish(data) {
          return pub.publish(key, JSON.stringify(data));
        },
        listener(pattern, fn) {
          sub.psubscribe(pattern);
          if (fn) {
            return new Observable(function channelObserver(obs) {
              sub.on("pmessage", (pattern, channel, message) => {
                fn(JSON.parse(message), { pattern, channel });
              });
            });
          } else {
            return new Observable(function channelObserver(obs) {
              sub.on("pmessage", (pattern, channel, message) => {
                obs.next(JSON.parse(message), { pattern, channel });
              });
            });
          }
        },
      };
    },
  };
};
