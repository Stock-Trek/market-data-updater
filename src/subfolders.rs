use crate::constants::{HOURS, MINUTES, MONTHS};
use anyhow::anyhow;

pub struct SubFolders;

impl SubFolders {
    pub fn subfolders_from_path(path: &str) -> anyhow::Result<Vec<String>> {
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        match segments.len() {
            0 | 1 | 2 | 3 | 4 => Ok(Vec::new()),
            5 => Ok(Self::subfolders(&MONTHS)),
            6 => {
                if let (Ok(year), Ok(month)) =
                    (segments[4].parse::<i32>(), segments[5].parse::<u32>())
                {
                    let days_in_month = Self::days_in_month(year, month);
                    let day_indexes: Vec<i32> = (1..=days_in_month).map(|d| d as i32).collect();
                    Ok(Self::subfolders(&day_indexes))
                } else {
                    Err(anyhow!(format!(
                        "Failed to parse year and month from path {}",
                        path
                    )))
                }
            }
            7 => Ok(Self::subfolders(&HOURS)),
            8 => Ok(Self::subfolders(&MINUTES)),
            9 => Ok(Vec::new()),
            _ => Err(anyhow!(format!(
                "Cannot get subfolders from path with too many delimiters {}",
                path
            ))),
        }
    }

    pub fn subfolders(numbers: &[i32]) -> Vec<String> {
        numbers.iter().map(|n| format!("{:02}/", n)).collect()
    }

    pub fn file_path(numbers: &[i32], suffix: &str) -> String {
        let raw_file_path = numbers
            .iter()
            .map(|n| format!("{:02}", n))
            .collect::<Vec<_>>()
            .join("/");
        format!("{}{}", raw_file_path, suffix)
    }

    fn days_in_month(year: i32, month: u32) -> u32 {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            2 => {
                if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                    29
                } else {
                    28
                }
            }
            _ => unreachable!("Month {} must be between 1 and 12", month),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> &'static str {
        "paper/Binance/Bitcoin/TetherUSD"
    }

    #[test]
    fn year_expects_every_calendar_month() {
        let subfolders = SubFolders::subfolders_from_path(&format!("{}/2024", base())).unwrap();
        assert_eq!(subfolders.len(), 12);
        assert_eq!(subfolders.first().unwrap(), "01/");
        assert_eq!(subfolders.last().unwrap(), "12/");
        assert!(!subfolders.contains(&"00/".to_string()));
    }

    #[test]
    fn month_expects_every_day_of_that_month() {
        let february = SubFolders::subfolders_from_path(&format!("{}/2024/02", base())).unwrap();
        assert_eq!(february.len(), 29);
        assert_eq!(february.first().unwrap(), "01/");
        assert_eq!(february.last().unwrap(), "29/");

        let april = SubFolders::subfolders_from_path(&format!("{}/2024/04", base())).unwrap();
        assert_eq!(april.len(), 30);
    }

    #[test]
    fn day_expects_every_hour() {
        let subfolders =
            SubFolders::subfolders_from_path(&format!("{}/2024/02/01", base())).unwrap();
        assert_eq!(subfolders.len(), 24);
        assert_eq!(subfolders.first().unwrap(), "00/");
        assert_eq!(subfolders.last().unwrap(), "23/");
    }

    #[test]
    fn hour_expects_every_six_minute_slot() {
        let subfolders =
            SubFolders::subfolders_from_path(&format!("{}/2024/02/01/03", base())).unwrap();
        assert_eq!(subfolders.len(), 10);
        assert_eq!(subfolders.first().unwrap(), "00/");
        assert_eq!(subfolders.last().unwrap(), "54/");
    }
}
