use crate::constants::{HOURS, MINUTES, MONTHS};
use anyhow::anyhow;

pub fn get_subfolders_from_path(path: &str) -> anyhow::Result<Vec<String>> {
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
    match segments.len() {
        0 | 1 | 2 | 3 => Ok(Vec::new()),
        4 => Ok(subfolders(&MONTHS)),
        5 => {
            if let (Ok(year), Ok(month)) = (segments[0].parse::<i32>(), segments[1].parse::<u32>())
            {
                let days_in_month = get_days_in_month(year, month);
                let day_indexes: Vec<i32> = (1..=days_in_month).map(|d| d as i32).collect();
                Ok(subfolders(&day_indexes))
            } else {
                Err(anyhow!(format!(
                    "Failed to parse year and month from path {}",
                    path
                )))
            }
        }
        6 => Ok(subfolders(&HOURS)),
        7 => Ok(subfolders(&MINUTES)),
        8 => Ok(Vec::new()),
        _ => Err(anyhow!(format!(
            "Cannot get subfolders from path with too many delimiters {}",
            path
        ))),
    }
}

pub fn subfolders(numbers: &[i32]) -> Vec<String> {
    numbers.iter().map(|n| format!("{:02}/", n)).collect()
}

pub fn file_path(numbers: &[i32], prefix: &str) -> String {
    let raw_file_path = numbers
        .iter()
        .map(|n| format!("{:02}", n))
        .collect::<Vec<_>>()
        .join("/");
    format!("{}{}", raw_file_path, prefix)
}

fn get_days_in_month(year: i32, month: u32) -> u32 {
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
        _ => 31,
    }
}
