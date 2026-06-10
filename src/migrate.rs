/*
    Project: Lotide Server
    ----------------------

    File: migrate.rs

    Purpose:

        Wire the compiled-in SQL migrations into migrant_lib and apply or
        roll them back from the Lotide command line.

    Responsibilities:

        - translate DATABASE_URL into migrant_lib connection settings
        - register embedded up/down migrations in stable tag order
        - apply migration actions requested by the command-line interface

    This file intentionally does NOT contain:

        - application startup
        - runtime database pooling
        - migration SQL generation
*/

pub const MIGRATIONS: &[StaticMigration] = include!(concat!(env!("OUT_DIR"), "/migrations.rs"));

pub struct StaticMigration {
    pub tag: &'static str,
    pub up: &'static str,
    pub down: &'static str,
}

fn migration_sql(sql: &'static str) -> &'static str {
    /*
        Some old migrations were edited on Windows and can carry a UTF-8 byte
        order mark. PostgreSQL treats that marker as part of the first token,
        so the BOM has to be removed before the SQL reaches migrant_lib.
    */
    sql.strip_prefix('\u{feff}').unwrap_or(sql)
}

pub fn run(
    config: crate::Config,
    matches: &clap::ArgMatches,
) -> Result<(), Box<dyn std::error::Error>> {
    let action = matches
        .get_one::<String>("ACTION")
        .map_or("up", std::string::String::as_str);

    let db_cfg: tokio_postgres::Config = config
        .database_url
        .parse()
        .expect("Failed to parse DATABASE_URL");

    let mut settings = migrant_lib::Settings::configure_postgres();
    settings
        .database_name(db_cfg.get_dbname().expect("Missing dbname"))
        .database_user(db_cfg.get_user().expect("Missing user"))
        .database_password(
            std::str::from_utf8(db_cfg.get_password().expect("Missing password")).unwrap(),
        );

    if let Some(path) = config.database_certificate_path {
        settings.ssl_cert_file(path);
    }

    let hosts = db_cfg.get_hosts();
    if !hosts.is_empty() {
        let host = hosts
            .iter()
            .find(|host| matches!(host, tokio_postgres::config::Host::Tcp(_)));

        match host {
            Some(tokio_postgres::config::Host::Tcp(hostname)) => {
                settings.database_host(hostname);
            }
            #[cfg(unix)]
            Some(_) | None => return Err("Unsupported host type".into()),
            #[cfg(not(unix))]
            None => return Err("Unsupported host type".into()),
        }
    }

    let ports = db_cfg.get_ports();
    if !ports.is_empty() {
        if ports.len() == 1 {
            settings.database_port(ports[0]);
        } else {
            return Err("Multiple ports are not supported".into());
        }
    }

    let settings = settings.build().unwrap();

    let mut config = migrant_lib::Config::with_settings(&settings);
    config.use_cli_compatible_tags(true);

    if action == "setup" {
        config.setup().expect("Failed to setup database");
    } else {
        let migrations: Vec<_> = MIGRATIONS
            .iter()
            .map(|item| {
                migrant_lib::EmbeddedMigration::with_tag(item.tag)
                    .up(migration_sql(item.up))
                    .down(migration_sql(item.down))
                    .boxed()
            })
            .collect();
        config
            .use_migrations(&migrations)
            .expect("Failed to initialize migrations");

        let config = config.reload().expect("Failed to check status");

        match action {
            "up" => {
                log::debug!("Applying migrations...");
                migrant_lib::Migrator::with_config(&config)
                    .all(true)
                    .swallow_completion(true)
                    .apply()
                    .expect("Failed to apply migrations");
            }
            "down" => {
                log::debug!("Unapplying migration...");
                migrant_lib::Migrator::with_config(&config)
                    .direction(migrant_lib::Direction::Down)
                    .all(false)
                    .swallow_completion(true)
                    .apply()
                    .expect("Failed to undo migration");
            }
            _ => return Err("Unknown migrate action".into()),
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn migration_sql_removes_utf8_bom() {
        assert_eq!(
            super::migration_sql("\u{feff}CREATE TABLE item ();"),
            "CREATE TABLE item ();"
        );
        assert_eq!(
            super::migration_sql("CREATE TABLE item ();"),
            "CREATE TABLE item ();"
        );
    }
}

/* end of migrate.rs */
