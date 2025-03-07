import { StrictFilter, StrictUpdateFilter, Document, UpdateFilter, AnyBulkWriteOperation, TransactionOptions, ClientSession } from "mongodb";
import DBManager from "../db/DBManager";
import ITransaction from "../models/transaction";
import IUser from "../models/user";
import IQuote from "../models/quote";

async function updateTransaction(ticker: string, currentPrice: number, session: ClientSession) {
    try {
        const filter: StrictFilter<ITransaction> = {ticker: ticker, done: false};
        const pipeline: Array<Document> = [
            { 
                $set: { 
                    PnL: {$multiply: [100000, { $subtract: [currentPrice, "$price"] }, "$lot"]}
                }
            }
        ]
        const result = DBManager.getInstance().collections.transaction?.updateMany(filter, pipeline, { session: session });
        return result;
    } catch (error) {
        throw error;
    }
}

async function bulkUpdateTransaction(quotes: Array<IQuote>, session: ClientSession) {
    try {
        let bulkUpdateList: AnyBulkWriteOperation<ITransaction>[] = [];
        quotes.forEach((quote) => {
            bulkUpdateList.push(
                {
                    "updateMany" : {
                        filter: { ticker: quote.p, done: false },
                        update: [
                            {
                                $set: {
                                    PnL: { $multiply: [100000, { $subtract: [quote.b, "$price"] }, "$lot"] }
                                }
                            }
                        ]
                    }
                }
            )
        })
        await DBManager.getInstance().collections.transaction?.bulkWrite(bulkUpdateList, { ordered: false, session: session });
    } catch (error) {
        throw error;
    }
}

async function getTransaction(ticker: string) {
    try {
        const filter: StrictFilter<ITransaction> = {ticker: ticker};
        const result = DBManager.getInstance().collections.transaction?.findOne(filter);
        return result;
    } catch (error) {
        throw error;
    }
}


async function getTransactionGroupByUser(session: ClientSession) {
    try {
        const pipeline: Array<Document> = [
            {
                $match : { "done": false }
            },
            {
                $group: {
                    _id : "$userId",
                    totalPnL: { $sum: "$PnL" },
                    totalLot: { $sum: { $abs: "$lot" } }
                }
            }
        ]
        const result = await DBManager.getInstance().collections.transaction?.aggregate(pipeline, { session: session }).toArray();
        return result;
    } catch (error) {
        throw error;
    }
}


async function bulkUpdateUser(data: Document[], session: ClientSession) {
    try {
        let bulkUpdateList: AnyBulkWriteOperation<IUser>[] = [];
        data.forEach(async (doc)=>{
            bulkUpdateList.push(
                {
                    updateOne: {
                        filter: { "_id": doc._id },
                        update: [
                            {
                                $set: { 
                                    "equity": { $add: [doc.totalPnL, { $multiply: [100000, doc.totalLot] }, "$balance"] },
                                    "unrealizedPnL": doc.totalPnL,
                                }
                            }
                        ]
                    }
                }
            );
            if (bulkUpdateList.length == 1000) {
                await DBManager.getInstance().collections.user?.bulkWrite(bulkUpdateList, { ordered : false });
                bulkUpdateList = [];
            }
        })
        if (bulkUpdateList.length > 0) await DBManager.getInstance().collections.user?.bulkWrite(bulkUpdateList, { ordered : false, session: session });
    } catch (error) {
        throw error;
    }
}

export async function updateTransactionAndUser(quote: IQuote) {
    if (DBManager.getInstance().client) {
        const session = DBManager.getInstance().client!.startSession();
        const transactionOptions: TransactionOptions = {
            readPreference: 'primary',
            readConcern: { level: 'local' },
            writeConcern: { w: 'majority' }
        };
        try {
            await session.withTransaction(async () => {     
                const ticker = quote.p.replace("/", "");
                const bidPrice = quote.b; 
                const updateTransactionResult = await updateTransaction(ticker, bidPrice, session);
                const transactionGroupByUser = await getTransactionGroupByUser(session);
                if (transactionGroupByUser) {
                    await bulkUpdateUser(transactionGroupByUser, session);
                }
            }, transactionOptions);
        } catch (error) {
            await session.abortTransaction();
            throw error;
        } finally {
            await session.endSession();
        }
    }
}