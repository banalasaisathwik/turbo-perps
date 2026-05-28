import Redis from "ioredis";
import type { EngineCommandType, EngineRequest, EngineResponse } from "../types/engine";
import { resolveEngineResponse, waitForEngineResponse } from "../store/pendingResolve";

const publisher = new Redis(process.env.REDIS_URL!).on("error", (error) => {
  console.error("Redis broker client error", error);
});;
const subscriber = new Redis(process.env.REDIS_URL!).on("error", (error) => {
  console.error("Redis broker client error", error);
});;

const enginequeue = process.env.ENGINE_QUEUE ?? "backend-engine-queue";
const backendQueue = process.env.BACKEND_QUEUE_ID ?? "backend1";

export async function sendToEngine(
  type: EngineCommandType, 
  payload: Record<string, unknown>
): Promise<EngineResponse> {

  const correlationId = crypto.randomUUID();
  const responsePromise = waitForEngineResponse(correlationId, 300000);

  const message: EngineRequest = {
    correlationId,
    responseQueue: backendQueue,
    type,
    payload,
  };

  // FIX: ioredis uses lowercase `.xadd` and takes arguments as sequential strings/flat array
  await publisher.xadd(enginequeue, "*", "data", JSON.stringify(message));
  
  return responsePromise;
}

export async function listenForEngineResponse() {
  let lastId = "0";
  for (;;) {
    try {
      // FIX: ioredis uses `.xread` passing CLI arguments sequentially. 
      // Syntax: xread("BLOCK", timeout, "COUNT", count, "STREAMS", key1, id1)
      const items = await subscriber.xread( 
        "COUNT", "10", 
        "BLOCK", "1000",
        "STREAMS", backendQueue, lastId
      ) as unknown as [streamName: string, messages: [messageId: string, keyValuePairs: string[]][]][] | null;

      // Null check handles timeouts cleanly when no messages arrive
      if (!items) {
        continue;
      }
      let latestBatchId = lastId

      // FIX: ioredis returns deeply nested arrays, not object structures
      for (const streamData of items) {
        const [streamName, messages] = streamData;

        for (const messageData of messages) {
          const [messageId, rawKeyValuePairs] = messageData;
          latestBatchId = messageId
          // Safely parse the sequential flat array into a JavaScript Object map
          const parsedFields: Record<string, string> = {};
          for (let i = 0; i < rawKeyValuePairs.length; i += 2) {
            const key = rawKeyValuePairs[i];
            const value = rawKeyValuePairs[i + 1];
            if (key !== undefined && value !== undefined) {
              parsedFields[key] = value;
            }
          }

          // Double check the stringified property key matches the 'data' field used during xadd
          if (parsedFields.data) {
            const parsedResponse = JSON.parse(parsedFields.data) as EngineResponse;
            resolveEngineResponse(parsedResponse);
          }
        }
      }
      lastId = latestBatchId;
    } catch (error) {
      console.error("Invalid engine response parsing or stream error:", error);
      // Brief pause preventing rapid loop lockups if the server drops unexpectedly
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
  }
}
