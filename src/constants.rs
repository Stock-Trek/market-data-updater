use crate::quantized_time::{Quantization, TopOfMinute};
use digdigdig3::{ExchangeId, Symbol};
use std::collections::HashMap;

pub const S3_BUCKET: &str = "stock-trek-market-data";
pub const PARQUET_SUFFIX: &str = ".parquet";

pub const UPDATE_QUANTIZATION: Quantization = Quantization::Minutes(TopOfMinute::Six);
pub const KLINE_LIMIT: Option<u16> = Some(10);

pub const MINUTES: [i32; 10] = [0, 6, 12, 18, 24, 30, 36, 42, 48, 54];
pub const HOURS: [i32; 6] = [0, 4, 8, 12, 16, 20];
pub const MONTHS: [i32; 4] = [0, 3, 6, 9];

const COMMON_SYMBOLS: &[(TestReal, ExchangeId, &str, &str)] = &[
    (TestReal::Test, ExchangeId::Coinbase, "BTC", "USDT"),
    (TestReal::Test, ExchangeId::Coinbase, "ETH", "USDT"),
    (TestReal::Real, ExchangeId::Coinbase, "BTC", "USDT"),
];

pub fn common_symbols() -> HashMap<(ExchangeId, TestReal), Vec<Symbol>> {
    let mut map = HashMap::new();
    for (test_real, exchange_id, base, quote) in COMMON_SYMBOLS {
        let key = (*exchange_id, *test_real);
        let symbol = Symbol::new(*base, *quote);
        let vec = map.entry(key).or_insert(Vec::new());
        vec.push(symbol);
    }
    map
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum TestReal {
    Test,
    Real,
}

impl std::fmt::Display for TestReal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestReal::Test => write!(f, "test"),
            TestReal::Real => write!(f, "real"),
        }
    }
}

impl Into<bool> for TestReal {
    fn into(self) -> bool {
        match self {
            TestReal::Test => true,
            TestReal::Real => false,
        }
    }
}
