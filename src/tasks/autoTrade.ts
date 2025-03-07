import { StrictFilter, StrictUpdateFilter, Document, UpdateFilter, AnyBulkWriteOperation, TransactionOptions, OptionalId, ClientSession } from "mongodb";
import DBManager from "../db/DBManager";
import { getTradeSignal } from "../api/tradeSignal";
import ITransaction from "../models/transaction";
import IUser from "../models/user";

async function stopTransaction(ticker: string, action: string, previousCloseDate: Date, session: ClientSession) {
    try {
        const filter: StrictFilter<ITransaction> = {ticker: ticker, action: { $ne: action }, done: false};
        const pipeline: Array<Document> = [
            {
                "$set": {
                    "done": true,
                    "endedAt": previousCloseDate
                }
            }
        ];
        const result = await DBManager.getInstance().collections.transaction?.updateMany(filter, pipeline, { session: session });
        return result;
    } catch (error) {
        throw error;
    }
}

async function getTransactionGroupByUser(ticker: string, previousCloseDate: Date, session: ClientSession) {
    try {
        const pipeline: Array<Document> = [
            {
                $match: {
                    "ticker": ticker,
                    "done": true,
                    "endedAt": previousCloseDate
                }
            },
            { 
                $group: {
                    "_id": "$userId",
                    "totalPnL": { $sum: "$PnL" },
                    "totalLot": { $sum: { $abs: "$lot" } }
                }
            }
        ];
        const result = DBManager.getInstance().collections.transaction?.aggregate(pipeline, { session: session }).toArray();
        return result;
    } catch (error) {
        throw error;
    }
}

async function bulkUpdateUser(data: Document[], session: ClientSession) {
    try {
        let bulkUpdateList: AnyBulkWriteOperation<IUser>[] = [];
        data.forEach(async (doc)=>{
            bulkUpdateList.push({
                updateOne: {
                    filter: { "userId": doc._id },
                    update: [
                        {
                            $set: { 
                                "equity": { $add: [doc.totalPnL, { $multiply: [100000, doc.totalLot] }, "$balance" ] },
                                "balance": { $add: [doc.totalPnL, { $multiply: [100000, doc.totalLot] }, "$balance" ] },
                                "unrealizedPnL": { $add: [-doc.totalPnL, "$unrealizedPnL"]}
                            }
                        }
                    ]
                }
            });
            if (bulkUpdateList.length == 1000) {
                await DBManager.getInstance().collections.user?.bulkWrite(bulkUpdateList, { ordered : false, session: session });
                bulkUpdateList = [];
            }
        })
        if (bulkUpdateList.length > 0) await DBManager.getInstance().collections.user?.bulkWrite(bulkUpdateList, { ordered : false, session: session });
    } catch (error) {
        throw error;
    }
}

async function getEligibleUser(ticker: string, session: ClientSession) {
    try {
        const pipeline: Array<Document> = [
            {
                $match: {
                    "ticker": ticker,
                    "status": "running"
                }
            },
            {
                $lookup: {
                    "from": "account",
                    "localField": "userId",
                    "foreignField": "_id",
                    "as": "account_matches"
                }
            },
            {
                $set: {
                    "account_matches": { $first: "$account_matches" } 
                }
            },
            {
                $match: {
                    $expr: {
                        $gte: [ "account_matches.balance", { $multiply: [100000, "$lot"] } ]
                    }
                }
            },
            {
                $project: {
                    "userId": 1,
                    "ticker": 1,
                    "lot": 1,
                    "_id": 0
                }
            }, 
            {
                $lookup: {
                    "from": "transaction",
                    "let": {
                        "userId": "$userId"
                    },
                    "pipeline": [
                       { 
                            $match: {
                                $expr: {
                                    $and: [
                                        { $eq: [ "$userId", "$$userId" ] },
                                        { $eq: [ "$ticker", ticker ] },
                                        { $eq: [ "$done", false ] }
                                    ]
                                }
                            }
                        }
                    ],
                    "as": "transaction_matches"
                }
            },
            {
                $match: {
                    "transaction_matches.userId": { $exists: false }
                }
            },
            {
                $project: {
                    "userId": 1,
                    "ticker": 1,
                    "lot": 1,
                    "_id": 0
                }
            }, 
        ];
        const result = await DBManager.getInstance().collections.subscription?.aggregate(pipeline, { session: session }).toArray();
        return result;
    } catch (error) {
        throw error;
    }
}

async function insertTransaction(data: Document[], action: string, previousClosePrice: number, previousCloseDate: Date, session: ClientSession) {
    try {
        let insertList: OptionalId<ITransaction>[] = [];
        const lotSign = action == "buy" ? 1 : -1;
        data.forEach(async (doc)=>{ 
            insertList.push({
                "ticker": doc.ticker,
                "price": previousClosePrice,
                "lot": lotSign * doc.lot,
                "action": action,
                "userId": doc.userId,
                "PnL": 0,
                "done": false,
                "createdAt": previousCloseDate
            })
        })
        const result = await DBManager.getInstance().collections.transaction?.insertMany(insertList, { session: session });
        return result;
    } catch (error) {
        throw error;
    }
}

async function bulkUpdateUserBalance(data: Document[], session: ClientSession) {
    try {
        let bulkUpdateList: AnyBulkWriteOperation<IUser>[] = [];
        data.forEach(async (doc)=>{
            bulkUpdateList.push({
                updateOne: {
                    filter: { "_id": doc.userId },
                    update: [{
                        $set: { 
                            "balance": { $add: [{ $multiply: [-100000, doc.lot] }, "$balance"] },
                        }
                    }]
                }
            });
            if (bulkUpdateList.length == 1000) {
                await DBManager.getInstance().collections.user?.bulkWrite(bulkUpdateList, { ordered : false, session: session });
                bulkUpdateList = [];
            }
        })
        if (bulkUpdateList.length > 0) await DBManager.getInstance().collections.user?.bulkWrite(bulkUpdateList, { ordered : false, session: session });
    } catch (error) {
        throw error;
    }
}

export async function autoTrade(ticker: string, tradeAction: string, prevClosePrice: number, createdAt: number) {
    if (DBManager.getInstance().client) {
        const createdAtDate = new Date(createdAt);
        const session = DBManager.getInstance().client!.startSession();
        const transactionOptions: TransactionOptions = {
            readPreference: 'primary',
            readConcern: { level: 'local' },
            writeConcern: { w: 'majority' }
        };
        try {
            await session.withTransaction(async () => {     
                await stopTransaction(ticker, tradeAction, createdAtDate, session);
                const transactionGroupByUser = await getTransactionGroupByUser(ticker, createdAtDate, session);
                if (transactionGroupByUser && transactionGroupByUser.length > 0) {
                    await bulkUpdateUser(transactionGroupByUser, session);
                } 
                const eligibleUsers = await getEligibleUser(ticker, session);
                if (eligibleUsers && eligibleUsers.length > 0) {
                    await insertTransaction(eligibleUsers, tradeAction, prevClosePrice, createdAtDate, session);
                    await bulkUpdateUserBalance(eligibleUsers, session);
                }
            }, transactionOptions);
        } catch (error) {
            await session.abortTransaction();
            throw error;
        } finally {
            await session.endSession();
        }
        return "Successful Auto Trade";
    }
}