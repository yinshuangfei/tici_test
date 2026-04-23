use crate::common::{format_table_target, quote_identifier};

pub fn drop_table_sql(database: &str, table_name: &str) -> Result<String, String> {
    Ok(format!(
        "DROP TABLE IF EXISTS {}.{};",
        quote_identifier(database)?,
        quote_identifier(table_name)?
    ))
}

pub fn build_drop_table_items(
    database: &str,
    table_names: &[String],
) -> Result<Vec<(String, String, String)>, String> {
    table_names
        .iter()
        .map(|table_name| {
            Ok((
                "drop-table".to_string(),
                format_table_target(database, table_name),
                drop_table_sql(database, table_name)?,
            ))
        })
        .collect()
}
