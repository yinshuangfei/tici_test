use crate::common::{format_table_target, quote_identifier};

pub fn create_table_sql(database: &str, table_name: &str) -> Result<String, String> {
    Ok(format!(
        "CREATE TABLE IF NOT EXISTS {}.{} (\n    id BIGINT AUTO_INCREMENT,\n    timestamp BIGINT,\n    severity_text VARCHAR(50),\n    body TEXT,\n    tenant_id INT,\n    PRIMARY KEY (tenant_id, id)\n);",
        quote_identifier(database)?,
        quote_identifier(table_name)?
    ))
}

pub fn build_create_table_items(
    database: &str,
    table_names: &[String],
) -> Result<Vec<(String, String, String)>, String> {
    table_names
        .iter()
        .map(|table_name| {
            Ok((
                "create-table".to_string(),
                format_table_target(database, table_name),
                create_table_sql(database, table_name)?,
            ))
        })
        .collect()
}
