use std::env;
use crate::cfg::load_and_merge_config;

#[cfg(test)]
mod tests {
    use serial_test::serial; // for setting env vars
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::{tempdir, TempDir};

    fn clear_snapchain_env_vars() {
        let keys_to_clear: Vec<String> = env::vars()
            .filter(|(key, _)| key.starts_with("SNAPCHAIN_"))
            .map(|(key, _)| key)
            .collect();

        for key in keys_to_clear {
            env::remove_var(key);
        }
    }

    struct EnvVarGuard;

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            clear_snapchain_env_vars();
        }
    }

    fn run_test<T>(test: T)
    where
        T: FnOnce(),
    {
        clear_snapchain_env_vars();
        let _guard = EnvVarGuard;
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
        run_test(|| {
            let args = vec!["test_binary".to_string()];

            let config = load_and_merge_config(args).expect("Failed to load config");

            assert_eq!(config.id, 0);
            assert_eq!(config.log_format, "text");

            // subsection
            assert_eq!(config.fnames.disable, false);
            assert_eq!(config.fnames.start_from, 0);
            assert_eq!(config.fnames.stop_at, 200);
            assert_eq!(config.fnames.url, "https://fnames.farcaster.xyz/transfers".to_string());
        })
    }

    #[test]
    #[serial]
    fn test_load_with_config_file() {
        run_test(|| {
            let (tmpdir, file_path) = write_config_file(r#"
                id = 42
                log_format = "json"
            "#);

            let args = vec![
                "test_binary".to_string(),
                "--config-path".to_string(),
                file_path.to_string(),
            ];

            let config = load_and_merge_config(args).expect("Failed to load config");

            assert_eq!(config.id, 42);
            assert_eq!(config.log_format, "json");
        })
    }

    #[test]
    #[serial]
    fn test_load_with_env_vars() {
        run_test(|| {
            let (tmpdir, file_path) = write_config_file(r#"
                id = 42
                log_format = "text"
            "#);

            env::set_var("SNAPCHAIN_ID", "43");
            env::set_var("SNAPCHAIN_LOG_FORMAT", "json");

            let args = vec![
                "test_binary".to_string(),
                "--config-path".to_string(),
                file_path.to_string(),
            ];
            let config = load_and_merge_config(args).expect("Failed to load config");

            assert_eq!(config.id, 43);
            assert_eq!(config.log_format, "json");
        })
    }

    #[test]
    #[serial]
    fn test_cli_overrides() {
        run_test(|| {
            let (tmpdir, file_path) = write_config_file(r#"
                id = 42
                log_format = "text"
            "#);

            env::set_var("SNAPCHAIN_ID", "43");
            env::set_var("SNAPCHAIN_LOG_FORMAT", "text");

            let args = vec![
                "test_binary".to_string(),
                "--config-path".to_string(),
                file_path.to_string(),
                "--id".to_string(),
                "100".to_string(),
                "--log-format".to_string(),
                "json".to_string(),
            ];

            let config = load_and_merge_config(args).expect("Failed to load config");

            assert_eq!(config.id, 100);
            assert_eq!(config.log_format, "json");
        })
    }

    #[test]
    #[serial]
    fn test_subsection_config() {
        run_test(|| {
            let (tmpdir, file_path) = write_config_file(r#"
                id = 42
                log_format = "text"

                [fnames]
                disable = true
                start_from = 100
                stop_at = 150
                url = "http://example.com/hello/world"
            "#);

            env::set_var("SNAPCHAIN_FNAMES__URL", "http://example.com/hello/universe");

            let args = vec![
                "test_binary".to_string(),
                "--config-path".to_string(),
                file_path.to_string(),
            ];

            let config = load_and_merge_config(args).expect("Failed to load config");
            assert_eq!(config.fnames.disable, true);
            assert_eq!(config.fnames.start_from, 100);
            assert_eq!(config.fnames.stop_at, 150);
            assert_eq!(config.fnames.url, "http://example.com/hello/universe".to_string());
        })
    }


    #[test]
    #[serial]
    fn test_missing_config_file() {
        run_test(|| {
            let args = vec![
                "test_binary".to_string(),
                "--config-path".to_string(),
                "this-file-doesn't-exist-12345".to_string(),
            ];

            let result = load_and_merge_config(args);

            assert!(result.is_err(), "Expected an error due to missing config file, but got Ok");

            if let Err(e) = result {
                assert!(e.to_string().contains("config file not found"), "Unexpected error message: {}", e);
            }
        })
    }
}
