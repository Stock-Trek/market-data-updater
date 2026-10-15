use crate::{
    constants::{HOURS, MINUTES, MONTHS},
    error::{MduError, MduResult},
};

pub struct SubFolders;

impl SubFolders {
    pub fn subfolders_from_path(path: &str) -> MduResult<Vec<String>> {
        let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        match segments.len() {
            0 | 1 | 2 | 3 | 4 => Ok(Vec::new()),
            5 => Ok(Self::subfolders(&MONTHS)),
            6 => {
                let year = segments[4]
                    .parse::<i32>()
                    .map_err(|_| MduError::InvalidYearMonth {
                        path: path.to_string(),
                    })?;
                let month = segments[5]
                    .parse::<u32>()
                    .map_err(|_| MduError::InvalidYearMonth {
                        path: path.to_string(),
                    })?;
                let days_in_month = Self::days_in_month(year, month)?;
                let day_indexes: Vec<i32> = (1..=days_in_month).map(|d| d as i32).collect();
                Ok(Self::subfolders(&day_indexes))
            }
            7 => Ok(Self::subfolders(&HOURS)),
            8 => Ok(Self::subfolders(&MINUTES)),
            9 => Ok(Vec::new()),
            _ => Err(MduError::TooManyPathSegments {
                path: path.to_string(),
            }),
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

    fn days_in_month(year: i32, month: u32) -> MduResult<u32> {
        match month {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => Ok(31),
            4 | 6 | 9 | 11 => Ok(30),
            2 => {
                if (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0) {
                    Ok(29)
                } else {
                    Ok(28)
                }
            }
            _ => Err(MduError::InvalidMonth { month }),
        }
    }
}
