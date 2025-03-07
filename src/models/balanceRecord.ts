import { ObjectId } from "mongodb";

export default interface IBalanceRecord {
    _id?: ObjectId;
    userId?: ObjectId;
    action?: string;
    amount?: number;
    createdAt?: Date;
}