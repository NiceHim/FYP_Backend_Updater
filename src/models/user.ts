import { ObjectId } from "mongodb";

export default interface IUser {
    _id?: ObjectId;
    userName: string;
    hash: string;
    balance: number;
    equity: number;
    unrealizedPnL: number;
    createdAt: Date;
}