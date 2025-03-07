import { MongoClient, Collection, ObjectId, Db } from "mongodb";
import IUser from "../models/user";
import IBalanceRecord from "../models/balanceRecord";
import ISubscription from "../models/subscription";
import ITransaction from "../models/transaction";

class DBManager {
    private static dbManager: DBManager;
    private _client?: MongoClient;
    private _db?: Db;
    private mongoUrl: string = process.env.MONGODB_URI || "";
    private mongoDbName: string = process.env.MONGODB_DATABASE_NAME || "";
    private _collections: { 
        user?: Collection<IUser>; 
        balanceRecord?: Collection<IBalanceRecord>;
        subscription?: Collection<ISubscription>;
        transaction?: Collection<ITransaction>;
    } = {}

    private constructor() {
    }

    static getInstance(): DBManager {
        if (this.dbManager) {
            return this.dbManager;
        } else {
            this.dbManager = new DBManager();
            return this.dbManager;
        }
    }

    get collections() {
        return this._collections;
    }

    get client() {
        return this._client;
    }


    async connDB() {
        try {
            this._client = new MongoClient(this.mongoUrl);
            await this._client.connect();
            this._db = DBManager.getInstance().client!.db(this.mongoDbName);
            this._collections.user = this._db?.collection<IUser>("account");
            this._collections.balanceRecord = this._db?.collection<IBalanceRecord>("balanceRecord");
            this._collections.subscription = this._db?.collection<ISubscription>("subscription");
            this._collections.transaction = this._db?.collection<ITransaction>("transaction");
        } catch (error) {
            throw error;
        }
    }
    
    async disconnDB() {
        try {
            if (this._client) {
                await this._client.close();
            }
        } catch (error) {
            throw error;
        }
    }
}

export default DBManager;