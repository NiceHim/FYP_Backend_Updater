import { ObjectId } from "mongodb";

export default interface ITransaction {
    _id?: ObjectId;
    userId: ObjectId;
    ticker: string;
    action: string;
    price: number;
    lot: number;
    PnL: number;
    done: boolean;
    createdAt: Date;
    endedAt?: Date;
}