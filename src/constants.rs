use crate::quantized_time::{Quantization, TopOfMinute};
use exchange_types::urls::TradingMode;
use hashbrown::HashMap;
use stock_trek_types::cex::{asset_id::AssetId, cex_id::CexId, trading_pair::TradingPair};

pub const S3_BUCKET: &str = "stock-trek-market-data";
pub const PARQUET_SUFFIX: &str = ".parquet";
pub const LOCK_KEY: &str = "_locks/market-data-updater";

pub const UPDATE_QUANTIZATION: Quantization = Quantization::Minutes(TopOfMinute::Six);
pub const KLINE_LIMIT: Option<u16> = Some(10);

pub const MINUTES: [i32; 10] = [0, 6, 12, 18, 24, 30, 36, 42, 48, 54];
pub const HOURS: [i32; 24] = [
    0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23,
];
pub const MONTHS: [i32; 12] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12];

const COMMON_TRADING_PAIRS: &[(TradingMode, CexId, AssetId, AssetId)] = &[
    (
        TradingMode::Paper,
        CexId::Binance,
        AssetId::Bitcoin,
        AssetId::TetherUSD,
    ),
    (
        TradingMode::Paper,
        CexId::Binance,
        AssetId::Ethereum,
        AssetId::TetherUSD,
    ),
    (
        TradingMode::Real,
        CexId::Binance,
        AssetId::Bitcoin,
        AssetId::TetherUSD,
    ),
];

pub fn common_trading_pairs() -> HashMap<(CexId, TradingMode), Vec<TradingPair>> {
    let mut map = HashMap::new();
    for (trading_mode, cex_id, base, quote) in COMMON_TRADING_PAIRS {
        let key = (*cex_id, *trading_mode);
        let trading_pair = TradingPair::new(*base, *quote);
        let vec = map.entry(key).or_insert(Vec::new());
        vec.push(trading_pair);
    }
    map
}
