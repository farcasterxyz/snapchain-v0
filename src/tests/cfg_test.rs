use std::env;

#[derive(Debug)]
struct Env(String, Option<String>);

fn set(key: &str, val: &str) -> Env {
    Env(key.to_string(), Some(val.to_string()))
}

fn clr(key: &str) -> Env {
    Env(key.to_string(), None)
}

fn set_envs(envs: &Vec<Env>) {
    for Env(key, val) in envs {
        match val {
            Some(val) => env::set_var(key, val),
            None => env::remove_var(key),
        }
    }
}

struct EnvRestorer(Vec<Env>);

impl Drop for EnvRestorer {
    fn drop(&mut self) {
        set_envs(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cfg::load_and_merge_config;
    use serial_test::serial; // for setting env vars
    use std::fs::File;
    use std::io::Write;
    use tempfile::{tempdir, TempDir};

    fn run_test<T>(envs: Vec<Env>, test: T)
    where
        T: FnOnce(),
    {
        let mut restore: Vec<Env> = Vec::new();
        for Env(key, _) in &envs {
            let existing = env::var(key);
            let to_push: Env = match existing {
                Ok(val) => set(key, val.as_str()),
                Err(_) => clr(key),
            };
            restore.push(to_push);
        }
        let _restorer = EnvRestorer(restore);

        set_envs(&envs);

        test()
    }

    fn write_config_file(content: &str) -> (TempDir, String) {
        let dir = tempdir().expect("Failed to create temp directory");
        let file_path = dir.path().join("config.toml");

        let mut file = File::create(&file_path).expect("Failed to create config file");
        writeln!(file, "{}", content).expect("Failed to write to config file");

        // return ownership of dir to delay cleanup
        (dir, file_path.to_str().unwrap().to_string())
    }

    #[test]
    #[serial]
    fn test_load_with_defaults() {
        run_test(vec![], || {
            let args = vec!["test_binary".to_string()];

            let result = load_and_merge_config(args);
            match result {
                Ok(_) => panic!("Expected error due to missing config, but got Ok"),
                Err(e) => {
                    assert!(e
                        .to_string()
                        .contains("error: the following required arguments were not provided:"));
                    assert!(e.to_string().contains("--config-path <CONFIG_PATH>"));
                }
            }
        })
    }

    #[test]
    #[serial]
    fn test_load_with_config_file() {
        run_test(vec![], || {
            let (_tmpdir, file_path) = write_config_file(
                r#"
                log_format = "json"
            "#,
            );

            let args = vec![
                "test_binary".to_string(),
                "--config-path".to_string(),
                file_path.to_string(),
            ];

            let config = load_and_merge_config(args).expect("Failed to load config");

            assert_eq!(config.log_format, "json");
        })
    }

    #[test]
    #[serial]
    fn test_load_with_env_vars() {
        run_test(
            vec![
                set("SNAPCHAIN_ID", "43"),
                set("SNAPCHAIN_LOG_FORMAT", "json"),
            ],
            || {
                let (_tmpdir, file_path) = write_config_file(
                    r#"
                log_format = "text"
            "#,
                );

                let args = vec![
                    "test_binary".to_string(),
                    "--config-path".to_string(),
                    file_path.to_string(),
                ];
                let config = load_and_merge_config(args).expect("Failed to load config");

                assert_eq!(config.log_format, "json");
            },
        )
    }

    #[test]
    #[serial]
    fn test_cli_overrides() {
        run_test(
            vec![
                set("SNAPCHAIN_ID", "43"),
                set("SNAPCHAIN_LOG_FORMAT", "text"),
            ],
            || {
                let (_tmpdir, file_path) = write_config_file(
                    r#"
                log_format = "text"
            "#,
                );

                let args = vec![
                    "test_binary".to_string(),
                    "--config-path".to_string(),
                    file_path.to_string(),
                    "--log-format".to_string(),
                    "json".to_string(),
                ];

                let config = load_and_merge_config(args).expect("Failed to load config");

                assert_eq!(config.log_format, "json");
            },
        )
    }

    #[test]
    #[serial]
    fn test_subsection_config() {
        run_test(
            vec![set(
                "SNAPCHAIN_FNAMES__URL",
                "http://example.com/hello/universe",
            )],
            || {
                let (_tmpdir, file_path) = write_config_file(
                    r#"
                log_format = "text"

                [fnames]
                disable = true
                start_from = 100
                stop_at = 150
                url = "http://example.com/hello/world"
            "#,
                );

                let args = vec![
                    "test_binary".to_string(),
                    "--config-path".to_string(),
                    file_path.to_string(),
                ];

                let config = load_and_merge_config(args).expect("Failed to load config");
                assert_eq!(config.fnames.disable, true);
                assert_eq!(config.fnames.start_from, 100);
                assert_eq!(config.fnames.stop_at, 150);
                assert_eq!(
                    config.fnames.url,
                    "http://example.com/hello/universe".to_string()
                );
            },
        )
    }

    #[test]
    #[serial]
    fn test_missing_config_file() {
        run_test(vec![], || {
            let args = vec![
                "test_binary".to_string(),
                "--config-path".to_string(),
                "this-file-doesn't-exist-12345".to_string(),
            ];

            let result = load_and_merge_config(args);

            assert!(
                result.is_err(),
                "Expected an error due to missing config file, but got Ok"
            );

            if let Err(e) = result {
                assert!(
                    e.to_string().contains("config file not found"),
                    "Unexpected error message: {}",
                    e
                );
            }
        })
    }
}
