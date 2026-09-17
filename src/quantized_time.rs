use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
pub enum Weekday {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
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

const SECONDS_PER_DAY: i64 = 24 * 60 * 60;
const SECONDS_PER_WEEK: i64 = 7 * SECONDS_PER_DAY;

impl QuantizedTime {
    pub fn timestamp(timestamp: SystemTime, quantization: Quantization) -> SystemTime {
        match quantization {
            Quantization::Minutes(top_of_minute) => Self::minute(timestamp, top_of_minute),
            Quantization::Hour(top_of_hour) => Self::hour(timestamp, top_of_hour),
            Quantization::Day => Self::day(timestamp),
            Quantization::Week(weekday) => Self::week(timestamp, weekday),
            Quantization::Month(top_of_month) => Self::month(timestamp, top_of_month),
            Quantization::Year => Self::year(timestamp),
        }
    }

    pub fn previous_top(top: SystemTime, quantization: Quantization) -> SystemTime {
        match quantization {
            Quantization::Minutes(top_of_minute) => {
                top - Duration::from_secs(top_of_minute as u64 * 60)
            }
            Quantization::Hour(top_of_hour) => {
                top - Duration::from_secs(top_of_hour as u64 * 60 * 60)
            }
            Quantization::Day => top - Duration::from_secs(SECONDS_PER_DAY as u64),
            Quantization::Week(_) => top - Duration::from_secs(SECONDS_PER_WEEK as u64),
            Quantization::Month(top_of_month) => Self::previous_month(top, top_of_month),
            Quantization::Year => Self::previous_year(top),
        }
    }

    fn minute(timestamp: SystemTime, top_of_minute: TopOfMinute) -> SystemTime {
        Self::modulo_seconds(timestamp, top_of_minute as i64 * 60)
    }

    fn hour(timestamp: SystemTime, top_of_hour: TopOfHour) -> SystemTime {
        Self::modulo_seconds(timestamp, top_of_hour as i64 * 60 * 60)
    }

    fn day(timestamp: SystemTime) -> SystemTime {
        Self::modulo_seconds(timestamp, SECONDS_PER_DAY)
    }

    fn week(timestamp: SystemTime, weekday: Weekday) -> SystemTime {
        let seconds = Self::to_seconds(timestamp);
        let days = seconds.div_euclid(SECONDS_PER_DAY);
        let current_weekday = (days + 3).rem_euclid(7);
        let days_to_subtract = (current_weekday - weekday as i64 + 7).rem_euclid(7);
        Self::from_seconds((days - days_to_subtract) * SECONDS_PER_DAY)
    }

    fn month(timestamp: SystemTime, top_of_month: TopOfMonth) -> SystemTime {
        let days = Self::to_seconds(timestamp).div_euclid(SECONDS_PER_DAY);
        let (year, _, _) = Self::civil_from_days(days);
        let month = top_of_month as u32 + 1;
        Self::from_seconds(Self::days_from_civil(year, month, 1) * SECONDS_PER_DAY)
    }

    fn year(timestamp: SystemTime) -> SystemTime {
        let days = Self::to_seconds(timestamp).div_euclid(SECONDS_PER_DAY);
        let (year, _, _) = Self::civil_from_days(days);
        Self::from_seconds(Self::days_from_civil(year, 1, 1) * SECONDS_PER_DAY)
    }

    fn previous_month(top: SystemTime, top_of_month: TopOfMonth) -> SystemTime {
        let seconds = Self::to_seconds(top);
        let days = seconds.div_euclid(SECONDS_PER_DAY);
        let seconds_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
        let (year, month, day) = Self::civil_from_days(days);
        let previous_month_raw = month as i64 - top_of_month as i64;
        let (previous_year, previous_month) = if previous_month_raw < 1 {
            (year - 1, previous_month_raw + 12)
        } else {
            (year, previous_month_raw)
        };
        let previous_days = Self::days_from_civil(previous_year, previous_month as u32, day);
        Self::from_seconds(previous_days * SECONDS_PER_DAY + seconds_of_day)
    }

    fn previous_year(top: SystemTime) -> SystemTime {
        let seconds = Self::to_seconds(top);
        let days = seconds.div_euclid(SECONDS_PER_DAY);
        let seconds_of_day = seconds.rem_euclid(SECONDS_PER_DAY);
        let (year, month, day) = Self::civil_from_days(days);
        let previous_days = Self::days_from_civil(year - 1, month, day);
        Self::from_seconds(previous_days * SECONDS_PER_DAY + seconds_of_day)
    }

    fn modulo_seconds(timestamp: SystemTime, modulo: i64) -> SystemTime {
        let timestamp_secs = Self::to_seconds(timestamp);
        Self::from_seconds(timestamp_secs - timestamp_secs.rem_euclid(modulo))
    }

    fn to_seconds(timestamp: SystemTime) -> i64 {
        match timestamp.duration_since(UNIX_EPOCH) {
            Ok(duration) => duration.as_secs() as i64,
            Err(error) => -(error.duration().as_secs() as i64),
        }
    }

    fn from_seconds(seconds: i64) -> SystemTime {
        if seconds >= 0 {
            UNIX_EPOCH + Duration::from_secs(seconds as u64)
        } else {
            UNIX_EPOCH - Duration::from_secs((-seconds) as u64)
        }
    }

    fn civil_from_days(days: i64) -> (i64, u32, u32) {
        let z = days + 719468;
        let era = if z >= 0 { z } else { z - 146096 } / 146097;
        let doe = (z - era * 146097) as u64;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
        let year = yoe as i64 + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
        let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
        (year + if month <= 2 { 1 } else { 0 }, month, day)
    }

    fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
        let year = if month <= 2 { year - 1 } else { year };
        let era = if year >= 0 { year } else { year - 399 } / 400;
        let yoe = (year - era * 400) as u64;
        let mp = if month > 2 { month - 3 } else { month + 9 } as u64;
        let doy = (153 * mp + 2) / 5 + day as u64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146097 + doe as i64 - 719468
    }
}
