import { ObjectId } from "mongodb";

export default interface ISubscription {
    _id?: ObjectId;
    userName: string;
    ticker: string;
    lot: number;
    done?: boolean;
    endedAt?: Date;
    createdAt: Date;
}