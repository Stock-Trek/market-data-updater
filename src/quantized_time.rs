use chrono::{DateTime, Datelike, Duration, TimeZone, Utc, Weekday};

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
    pub fn timestamp(timestamp: DateTime<Utc>, quantization: Quantization) -> DateTime<Utc> {
        match quantization {
            Quantization::Minutes(top_of_minute) => Self::minute(timestamp, top_of_minute),
            Quantization::Hour(top_of_hour) => Self::hour(timestamp, top_of_hour),
            Quantization::Day => Self::day(timestamp),
            Quantization::Week(weekday) => Self::week(timestamp, weekday),
            Quantization::Month(top_of_month) => Self::month(timestamp, top_of_month),
            Quantization::Year => Self::year(timestamp),
        }
    }

    pub fn previous_top(top: DateTime<Utc>, quantization: Quantization) -> DateTime<Utc> {
        match quantization {
            Quantization::Minutes(top_of_minute) => top - Duration::minutes(top_of_minute as i64),
            Quantization::Hour(top_of_hour) => top - Duration::hours(top_of_hour as i64),
            Quantization::Day => top - Duration::days(1),
            Quantization::Week(_) => top - Duration::weeks(1),
            Quantization::Month(top_of_month) => {
                let total_months =
                    top.year() as i64 * 12 + top.month0() as i64 - top_of_month as i64;
                let previous_year = total_months.div_euclid(12) as i32;
                let previous_month = total_months.rem_euclid(12) as u32 + 1;
                top.with_year(previous_year)
                    .unwrap()
                    .with_month(previous_month)
                    .unwrap()
            }
            Quantization::Year => top.with_year(top.year() - 1).unwrap(),
        }
    }

    fn minute(timestamp: DateTime<Utc>, top_of_minute: TopOfMinute) -> DateTime<Utc> {
        Self::modulo_seconds(timestamp, (top_of_minute as i64) * 60)
    }
    fn hour(timestamp: DateTime<Utc>, top_of_hour: TopOfHour) -> DateTime<Utc> {
        Self::modulo_seconds(timestamp, (top_of_hour as i64) * 60 * 60)
    }
    fn day(timestamp: DateTime<Utc>) -> DateTime<Utc> {
        Self::modulo_seconds(timestamp, 24 * 60 * 60)
    }
    fn week(timestamp: DateTime<Utc>, weekday: Weekday) -> DateTime<Utc> {
        let current_weekday = timestamp.weekday();
        let days_to_subtract = (current_weekday as i32 - weekday as i32 + 7) % 7;
        let time_delta_to_subtract = Duration::days(days_to_subtract as i64);
        timestamp
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .unwrap()
            .and_utc()
            .checked_sub_signed(time_delta_to_subtract)
            .unwrap()
    }
    fn month(timestamp: DateTime<Utc>, top_of_month: TopOfMonth) -> DateTime<Utc> {
        let year = timestamp.year();
        let month = timestamp.month0() / top_of_month as u32 * top_of_month as u32 + 1;
        Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap()
    }
    fn year(timestamp: DateTime<Utc>) -> DateTime<Utc> {
        let year = timestamp.year();
        Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).unwrap()
    }
    fn modulo_seconds(timestamp: DateTime<Utc>, modulo: i64) -> DateTime<Utc> {
        let timestamp_secs = timestamp.timestamp();
        let quantized_secs = timestamp_secs - timestamp_secs.rem_euclid(modulo);
        Utc.timestamp_opt(quantized_secs, 0).unwrap()
    }
}
