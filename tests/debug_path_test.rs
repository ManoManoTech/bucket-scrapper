use log_consolidator_checker_rust::config::loader::load_config;
use log_consolidator_checker_rust::utils::path_formatter::generate_path_formatter;
use tempfile::NamedTempFile;
use std::fs;

#[test]
fn test_path_generation_debug() {
    let config_content = r#"
bucketsToConsolidate:
  - bucket: "archived-bucket"
    path:
      - static_path: "logs"
      - datefmt: "2006/01/02/15"

bucketsConsolidated:
  - bucket: "consolidated-bucket"
    path:
      - static_path: "consolidated"
      - datefmt: "2006/01/02/15"

bucketsCheckerResults:
  - bucket: "results-bucket"
    path:
      - static_path: "results"
"#;
    
    let temp_config = NamedTempFile::new().unwrap();
    fs::write(temp_config.path(), config_content).unwrap();
    
    let config = load_config(temp_config.path()).unwrap();
    
    // Test archived bucket path generation
    let archived_bucket = &config.bucketsToConsolidate[0];
    let formatter = generate_path_formatter(archived_bucket);
    let path = formatter(&"20231225".to_string(), &"14".to_string()).unwrap();
    
    println!("Generated path for archived bucket: '{}'", path);
    println!("Expected prefix: 'logs/2023/12/25/14'");
    
    // Test consolidated bucket path generation
    let consolidated_bucket = &config.bucketsConsolidated[0];
    let formatter = generate_path_formatter(consolidated_bucket);
    let path = formatter(&"20231225".to_string(), &"14".to_string()).unwrap();
    
    println!("Generated path for consolidated bucket: '{}'", path);
    println!("Expected prefix: 'consolidated/2023/12/25/14'");
}