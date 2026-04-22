use std::collections::HashMap;

use digdigdig3::{ExchangeId, Symbol};

use crate::quantized_time::{Quantization, TopOfMinute};

pub const S3_BUCKET: &str = "stock-trek-market-data";
pub const PARQUET_SUFFIX: &str = ".parquet";

pub const UPDATE_QUANTIZATION: Quantization = Quantization::Minutes(TopOfMinute::Six);
pub const KLINE_LIMIT: Option<u16> = Some(10);

pub const MINUTES: [i32; 10] = [0, 6, 12, 18, 24, 30, 36, 42, 48, 54];
pub const HOURS: [i32; 6] = [0, 4, 8, 12, 16, 20];
pub const MONTHS: [i32; 4] = [0, 3, 6, 9];

const COMMON_EXCHANGE_SYMBOLS: &[(ExchangeId, &[(&str, &[&str])])] = &[
    (ExchangeId::Binance, &[("BTC", &["USDT"])]),
    (ExchangeId::Coinbase, &[("BTC", &["USD"])]),
];

pub fn common_symbols() -> HashMap<ExchangeId, Vec<Symbol>> {
    COMMON_EXCHANGE_SYMBOLS
        .iter()
        .map(|(exchange, symbols)| {
            let symbol_list = symbols
                .iter()
                .flat_map(|(base, quotes)| quotes.iter().map(|&quote| Symbol::new(*base, quote)))
                .collect();
            (*exchange, symbol_list)
        })
        .collect()
}
