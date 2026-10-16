use chrono::{DateTime, Datelike, Duration, TimeZone, Utc, Weekday};

use crate::error::{MduError, MduResult};

pub struct QuantizedTime;

#[allow(dead_code)]
pub enum Quantization {
    Minutes(TopOfMinute),
    Hour(TopOfHour),
    Day,
    Week(Weekday),
    Month(TopOfMonth),
    Year,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum TopOfMinute {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Five = 5,
    Six = 6,
    Ten = 10,
    Twelve = 12,
    Fifteen = 15,
    Twenty = 20,
    Thirty = 30,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum TopOfHour {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Six = 6,
    Twelve = 12,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum TopOfMonth {
    One = 1,
    Two = 2,
    Three = 3,
    Four = 4,
    Six = 6,
}

impl QuantizedTime {
    pub fn timestamp(
        timestamp: DateTime<Utc>,
        quantization: Quantization,
    ) -> MduResult<DateTime<Utc>> {
        match quantization {
            Quantization::Minutes(top_of_minute) => Self::minute(timestamp, top_of_minute),
            Quantization::Hour(top_of_hour) => Self::hour(timestamp, top_of_hour),
            Quantization::Day => Self::day(timestamp),
            Quantization::Week(weekday) => Self::week(timestamp, weekday),
            Quantization::Month(top_of_month) => Self::month(timestamp, top_of_month),
            Quantization::Year => Self::year(timestamp),
        }
    }

    pub fn next_top(top: DateTime<Utc>, quantization: Quantization) -> MduResult<DateTime<Utc>> {
        match quantization {
            Quantization::Minutes(top_of_minute) => {
                Self::checked_add(top, Duration::minutes(top_of_minute as i64))
            }
            Quantization::Hour(top_of_hour) => {
                Self::checked_add(top, Duration::hours(top_of_hour as i64))
            }
            Quantization::Day => Self::checked_add(top, Duration::days(1)),
            Quantization::Week(_) => Self::checked_add(top, Duration::weeks(1)),
            Quantization::Month(top_of_month) => {
                let total_months =
                    top.year() as i64 * 12 + top.month0() as i64 + top_of_month as i64;
                let next_year = total_months.div_euclid(12) as i32;
                let next_month = total_months.rem_euclid(12) as u32 + 1;
                Self::with_year_and_month(top, next_year, next_month)
            }
            Quantization::Year => Self::with_year_and_month(top, top.year() + 1, top.month()),
        }
    }

    pub fn previous_top(
        top: DateTime<Utc>,
        quantization: Quantization,
    ) -> MduResult<DateTime<Utc>> {
        match quantization {
            Quantization::Minutes(top_of_minute) => {
                Self::checked_sub(top, Duration::minutes(top_of_minute as i64))
            }
            Quantization::Hour(top_of_hour) => {
                Self::checked_sub(top, Duration::hours(top_of_hour as i64))
            }
            Quantization::Day => Self::checked_sub(top, Duration::days(1)),
            Quantization::Week(_) => Self::checked_sub(top, Duration::weeks(1)),
            Quantization::Month(top_of_month) => {
                let total_months =
                    top.year() as i64 * 12 + top.month0() as i64 - top_of_month as i64;
                let previous_year = total_months.div_euclid(12) as i32;
                let previous_month = total_months.rem_euclid(12) as u32 + 1;
                Self::with_year_and_month(top, previous_year, previous_month)
            }
            Quantization::Year => Self::with_year_and_month(top, top.year() - 1, top.month()),
        }
    }

    fn minute(timestamp: DateTime<Utc>, top_of_minute: TopOfMinute) -> MduResult<DateTime<Utc>> {
        Self::modulo_seconds(timestamp, (top_of_minute as i64) * 60)
    }
    fn hour(timestamp: DateTime<Utc>, top_of_hour: TopOfHour) -> MduResult<DateTime<Utc>> {
        Self::modulo_seconds(timestamp, (top_of_hour as i64) * 60 * 60)
    }
    fn day(timestamp: DateTime<Utc>) -> MduResult<DateTime<Utc>> {
        Self::modulo_seconds(timestamp, 24 * 60 * 60)
    }
    fn week(timestamp: DateTime<Utc>, weekday: Weekday) -> MduResult<DateTime<Utc>> {
        let current_weekday = timestamp.weekday();
        let days_to_subtract = (current_weekday as i32 - weekday as i32 + 7) % 7;
        let time_delta_to_subtract = Duration::days(days_to_subtract as i64);
        let day_start = timestamp
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .ok_or(MduError::DateTimeOutOfRange {
                operation: "find start of week",
            })?
            .and_utc();
        Self::checked_sub(day_start, time_delta_to_subtract)
    }
    fn month(timestamp: DateTime<Utc>, top_of_month: TopOfMonth) -> MduResult<DateTime<Utc>> {
        let year = timestamp.year();
        let month = timestamp.month0() / top_of_month as u32 * top_of_month as u32 + 1;
        Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0)
            .single()
            .ok_or(MduError::DateTimeOutOfRange {
                operation: "find start of month",
            })
    }
    fn year(timestamp: DateTime<Utc>) -> MduResult<DateTime<Utc>> {
        let year = timestamp.year();
        Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0)
            .single()
            .ok_or(MduError::DateTimeOutOfRange {
                operation: "find start of year",
            })
    }
    fn modulo_seconds(timestamp: DateTime<Utc>, modulo: i64) -> MduResult<DateTime<Utc>> {
        let timestamp_secs = timestamp.timestamp();
        let quantized_secs = timestamp_secs - timestamp_secs.rem_euclid(modulo);
        Utc.timestamp_opt(quantized_secs, 0)
            .single()
            .ok_or(MduError::DateTimeOutOfRange {
                operation: "quantize seconds",
            })
    }
    fn with_year_and_month(
        timestamp: DateTime<Utc>,
        year: i32,
        month: u32,
    ) -> MduResult<DateTime<Utc>> {
        timestamp
            .with_year(year)
            .and_then(|date_time| date_time.with_month(month))
            .ok_or(MduError::DateTimeOutOfRange {
                operation: "shift year and month",
            })
    }
    fn checked_add(timestamp: DateTime<Utc>, duration: Duration) -> MduResult<DateTime<Utc>> {
        timestamp
            .checked_add_signed(duration)
            .ok_or(MduError::DateTimeOutOfRange {
                operation: "add duration",
            })
    }
    fn checked_sub(timestamp: DateTime<Utc>, duration: Duration) -> MduResult<DateTime<Utc>> {
        timestamp
            .checked_sub_signed(duration)
            .ok_or(MduError::DateTimeOutOfRange {
                operation: "subtract duration",
            })
    }
}
