import "dotenv/config";
import { createOrder } from "./handler/create-order";
import { liquidation } from "./handler/liquidation";
import Redis from "ioredis";

export type EngineCommandType =
  | "create_order"
  | "get_depth"
  | "get_user_balance"
  | "get_order"
  | "cancel_order";

export interface OrderRequest {
  correlationId: string;
  responseQueue: string;
  type: EngineCommandType;
  payload: Record<string, unknown>;
}

export interface MarkPriceEvent {
  type: "mark_price";
  symbol: string;
  latestPrice: number;
}

export type BrokerMessage = OrderRequest | MarkPriceEvent

export interface EngineResponse {
  correlationId: string;
  ok: boolean;
  data?: unknown;
  error?: string;
}


// 1. Initialise ioredis Clients (Auto-connects natively, no explicit await .connect() needed)
const brokerClient = new Redis(process.env.REDIS_URL!).on("error", (error) => {
  console.error("Redis broker client error", error);
});

const responseClient = new Redis(process.env.REDIS_URL!).on("error", (error) => {
  console.error("Redis response client error", error);
});

const engineQueue = process.env.ENGINE_QUEUE ?? "backend-engine-queue";

async function sendResponse(responseQueue: string, response: EngineResponse): Promise<void> {
  // FIX: Lowercase .xadd and flat sequential key/value string arguments
  await responseClient.xadd(responseQueue, "*", "data", JSON.stringify(response));
}

function handleEngineRequest(message: OrderRequest): unknown {
  switch (message.type) {
    case "create_order":
      return createOrder(message); // Ensure createOrder is imported or defined
    default:
      break;
  }
}

console.log(`Engine listening on Redis queue: ${engineQueue}`);

// Initialize lastId before the loop (using "0" to fetch historic backlogs on start)
let lastId = "0"; 

for (;;) {
  try {
    // FIX: Sequential CLI arguments format for ioredis .xread
    const items = await brokerClient.xread(
      "COUNT", "10",
       "BLOCK", "1000",
      "STREAMS", engineQueue, lastId
    ) as unknown as [streamName: string, messages: [messageId: string, keyValuePairs: string[]][]][] | null;

    // Handle null or empty responses safely
    if (!items) continue;

    // Track the most recent ID processed in this batch
    let latestBatchId = lastId;

    for (const streamData of items) {
      const [streamName, messages] = streamData;

      for (const messageData of messages) {
        const [messageId, rawKeyValuePairs] = messageData;
        
        // Track the current message ID to update our stream pointer later
        latestBatchId = messageId;

        // FIX: Parse flat array structures [key, val, key, val] into a JavaScript object
         const parsedFields: Record<string, string> = {};
          for (let i = 0; i < rawKeyValuePairs.length; i += 2) {
            const key = rawKeyValuePairs[i];
            const value = rawKeyValuePairs[i + 1];
            if (key !== undefined && value !== undefined) {
              parsedFields[key] = value;
            }
          }

        // Guard against structural exceptions if the message doesn't contain a 'data' field
        if (!parsedFields.data) {
          console.error(`Skipping message ${messageId}: Missing 'data' payload key.`);
          continue;
        }

        let parsedData: BrokerMessage;
        try {
          parsedData = JSON.parse(parsedFields.data) as BrokerMessage;
        } catch (parseError) {
          console.error("Skipping invalid broker message JSON:", parseError);
          continue; 
        }

        if (parsedData.type === "mark_price") {
          try {
            liquidation(parsedData); // Ensure liquidation is imported or defined
          } catch (error) {
            console.error("Failed to process mark price", error);
          }
          continue; 
        }

        try {
          const data = handleEngineRequest(parsedData as unknown as OrderRequest);

          await sendResponse(parsedData.responseQueue, {
            correlationId: parsedData.correlationId,
            ok: true,
            data,
          });
        } catch (error) {
          await sendResponse(parsedData.responseQueue, {
            correlationId: parsedData.correlationId,
            ok: false,
            error: error instanceof Error ? error.message : "engine_error",
          });
        }
      }
    }

    // CRITICAL: Update pointers only after completing the processing loop successfully
    lastId = latestBatchId;

  } catch (loopError) {
    console.error("Critical error in batch loop execution:", loopError);
    // Brief fallback pause to stop high-speed error loops if Redis experiences severe network stuttering
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
}
