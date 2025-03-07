export default interface ITradeAction {
    ticker: string;
    action: string;
    prevClosePrice: number;
    createdAt: number;
}