use crate::{constants::KLINE_LIMIT, updaters::exchange_updater::ExchangeUpdater};
use anyhow::Result;
use chrono::{DateTime, Utc};
use exchange_gateway::{
    async_trait, clients::reqwest::ReqwestHttpClient, connector::Connector,
    submission::SubmissionOutcome,
};
use exchange_types::{
    binance::{
        exchange::BinanceExchange,
        klines::{BinanceKlinesInterval, BinanceKlinesRequest},
        klines_limit::BinanceKlinesLimit,
    },
    encode::ByteEncoder,
    klines::{Kline, KlinesResponse},
    new_types::Milliseconds,
    signer::Signer,
    urls::TradingMode,
};
use std::time::Duration;
use stock_trek_types::cex::trading_pair::TradingPair;
use type_bridge::{BinanceConverter, Converter};

const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_RETRIES: u8 = 3;

pub struct BinanceUpdater {
    converter: BinanceConverter,
    connector: Connector<BinanceExchange, ReqwestHttpClient>,
}

#[async_trait]
impl ExchangeUpdater for BinanceUpdater {
    async fn klines(
        &self,
        trading_pair: &TradingPair,
        start_time: DateTime<Utc>,
        end_time: DateTime<Utc>,
    ) -> Result<Vec<Kline>> {
        let Some(symbol) = self.converter.ticker_mapping().symbol(trading_pair) else {
            anyhow::bail!("Cannot find symbol for {:?}", trading_pair)
        };
        let mut request = BinanceKlinesRequest::new(symbol, BinanceKlinesInterval::ONE_MINUTE);
        let (start_millis, end_millis) = Self::kline_time_bounds(start_time, end_time);
        request.startTime = Some(Milliseconds(start_millis));
        request.endTime = Some(Milliseconds(end_millis));
        request.limit = KLINE_LIMIT
            .map(|limit| BinanceKlinesLimit::try_new(u32::from(limit)))
            .transpose()?;
        let submission = self.connector.submit_http(request)?;
        match submission.wait().await? {
            SubmissionOutcome::Submitted(response) => Ok(response.klines()?),
            SubmissionOutcome::Unknown { .. } => {
                anyhow::bail!("unknown Binance klines submission outcome")
            }
        }
    }
}

impl BinanceUpdater {
    fn kline_time_bounds(start_time: DateTime<Utc>, end_time: DateTime<Utc>) -> (i64, i64) {
        (
            start_time.timestamp_millis(),
            end_time.timestamp_millis() - 1,
        )
    }

    pub fn try_new(trading_mode: TradingMode) -> Result<Self> {
        let converter = BinanceConverter::default();
        let signer = Signer::new_unencrypted(ByteEncoder::HexLower);
        let connector = Connector::try_new_http_reqwest(
            trading_mode,
            BinanceExchange,
            signer,
            REQUEST_TIMEOUT,
            MAX_RETRIES,
        )?;
        Ok(Self {
            converter,
            connector,
        })
    }
}
