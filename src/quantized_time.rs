use chrono::{DateTime, Datelike, Duration, TimeZone, Utc, Weekday};

pub fn quantized_timestamp(timestamp: DateTime<Utc>, quantization: Quantization) -> DateTime<Utc> {
    match quantization {
        Quantization::Minutes(top_of_minute) => quantized_minute(timestamp, top_of_minute),
        Quantization::Hour(top_of_hour) => quantized_hour(timestamp, top_of_hour),
        Quantization::Day => quantized_day(timestamp),
        Quantization::Week(weekday) => quantized_week(timestamp, weekday),
        Quantization::Month(top_of_month) => quantized_month(timestamp, top_of_month),
        Quantization::Year => quantized_year(timestamp),
    }
}

pub fn previous_top(top: DateTime<Utc>, quantization: Quantization) -> DateTime<Utc> {
    match quantization {
        Quantization::Minutes(top_of_minute) => top - Duration::minutes(top_of_minute as i64),
        Quantization::Hour(top_of_hour) => top - Duration::hours(top_of_hour as i64),
        Quantization::Day => top - Duration::days(1),
        Quantization::Week(_) => top - Duration::weeks(1),
        Quantization::Month(top_of_month) => {
            let year = top.year();
            let previous_month_raw = top.month() - top_of_month as u32;
            let previous_year = if previous_month_raw < 1 {
                year - 1
            } else {
                year
            };
            let previous_month = if previous_month_raw < 1 {
                previous_month_raw + 12
            } else {
                previous_month_raw
            };
            top.with_year(previous_year)
                .unwrap()
                .with_month(previous_month)
                .unwrap()
        }
        Quantization::Year => top.with_year(top.year() - 1).unwrap(),
    }
}

fn quantized_minute(timestamp: DateTime<Utc>, top_of_minute: TopOfMinute) -> DateTime<Utc> {
    quantized_modulo_seconds(timestamp, (top_of_minute as i64) * 60)
}
fn quantized_hour(timestamp: DateTime<Utc>, top_of_hour: TopOfHour) -> DateTime<Utc> {
    quantized_modulo_seconds(timestamp, (top_of_hour as i64) * 60 * 60)
}
fn quantized_day(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    quantized_modulo_seconds(timestamp, 24 * 60 * 60)
}
fn quantized_week(timestamp: DateTime<Utc>, weekday: Weekday) -> DateTime<Utc> {
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
fn quantized_month(timestamp: DateTime<Utc>, top_of_month: TopOfMonth) -> DateTime<Utc> {
    let year = timestamp.year();
    let month = top_of_month as u32 + 1;
    Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap()
}
fn quantized_year(timestamp: DateTime<Utc>) -> DateTime<Utc> {
    let year = timestamp.year();
    Utc.with_ymd_and_hms(year, 1, 1, 0, 0, 0).unwrap()
}
fn quantized_modulo_seconds(timestamp: DateTime<Utc>, modulo: i64) -> DateTime<Utc> {
    let timestamp_secs = timestamp.timestamp();
    let quantized_secs = timestamp_secs - (timestamp_secs % modulo);
    Utc.timestamp_opt(quantized_secs, 0).unwrap()
}

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
