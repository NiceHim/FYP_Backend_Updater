import dotenv from "dotenv";
import path from "path";
import { createClient } from "redis";
import { updateTransactionAndUser } from "./src/tasks/updateTranscationAndUser";
import { autoTrade } from "./src/tasks/autoTrade";
import type IQuote from "./src/models/quote";
import type ITradeAction from "./src/models/tradeAction";
import DBManager from "./src/db/DBManager";

const env = process.env.NODE_ENV || "dev";
dotenv.config({ path: path.resolve(__dirname, `.env.${env}`) });

const redisURL = process.env.REDIS_URL || "redis://localhost:6379";
const subscriber = createClient({ url: redisURL });
subscriber.subscribe("forex.quote", async (message) => {
    const quote: IQuote = JSON.parse(message);
    await updateTransactionAndUser(quote);
})
subscriber.subscribe("forex.trade.action", async (message) => {
    const tradeAction: ITradeAction = JSON.parse(message);
    await autoTrade(tradeAction.ticker, tradeAction.action, tradeAction.prevClosePrice, tradeAction.createdAt);
})

DBManager.getInstance().connDB().then(() => {
    console.log("Database Connected");
    subscriber.connect().then(() => {
        console.log("Redis Connected");
    })
})