use chrono::NaiveDate;

pub(crate) fn normalize_date(date_str: &str) -> eyre::Result<String> {
    let formats = [
        "%d/%m/%Y",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%SZ",
        "%m/%d/%Y",
        "%d-%m-%Y",
    ];

    for &format in &formats {
        if let Ok(date) = NaiveDate::parse_from_str(date_str, format) {
            // let datetime = date.and_hms(0, 0, 0);
            // return Ok(datetime.format("%Y-%m-%d").to_string());
            return Ok(date.to_string());
        }
    }

    eyre::bail!("date time can not be supported");
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_transaction_store_new() -> eyre::Result<()> {
        let date_strings = [
            "07/07/2024",
            "2024-07-07",
            "2024-07-07T12:00:00Z",
            "07/07/2024",
            "07-07-2024",
        ];
        let checked_dt_str = "2024-07-07";
        for date_str in &date_strings {
            match normalize_date(date_str) {
                Ok(iso_date) => {
                    println!("Original: {}, ISO 8601 date: {}", date_str, iso_date);
                    assert_eq!(iso_date, checked_dt_str);
                }
                Err(e) => println!("Failed to parse {}: {:?}", date_str, e),
            }
        }

        Ok(())
    }
}
