//! Test fixtures: synthetic log lines + S3 seeding.

use anyhow::Result;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::Write;

#[derive(Clone, Copy, Debug)]
pub enum Encoding {
    Plain,
    Gzip,
    Zstd,
}

impl Encoding {
    pub fn extension(self) -> &'static str {
        match self {
            Encoding::Plain => "json",
            Encoding::Gzip => "json.gz",
            Encoding::Zstd => "json.zst",
        }
    }

    pub fn encode(self, plaintext: &str) -> Result<Vec<u8>> {
        match self {
            Encoding::Plain => Ok(plaintext.as_bytes().to_vec()),
            Encoding::Gzip => {
                let mut enc = GzEncoder::new(Vec::new(), Compression::default());
                enc.write_all(plaintext.as_bytes())?;
                Ok(enc.finish()?)
            }
            Encoding::Zstd => Ok(zstd::stream::encode_all(plaintext.as_bytes(), 3)?),
        }
    }
}

/// One staged S3 object: full key, raw line content (newline-joined), encoding.
pub struct StagedObject {
    pub key: String,
    pub lines: Vec<String>,
    pub encoding: Encoding,
}

impl StagedObject {
    pub fn plaintext(&self) -> String {
        let mut s = self.lines.join("\n");
        s.push('\n');
        s
    }

    pub fn body(&self) -> Result<Vec<u8>> {
        self.encoding.encode(&self.plaintext())
    }
}

/// Builds a deterministic fixture set covering plain / gzip / zstd, two date-hours,
/// and a couple of objects that should be filtered out by `only_prefix_patterns`.
///
/// Layout (under `logs/dt=YYYYMMDD/hour=HH/`):
///   - service-a-001.json.zst   — included
///   - service-a-002.json.gz    — included
///   - service-b-001.json       — included
///   - skip-me.txt              — filtered (no service- prefix and wrong ext)
///
/// Lines: each object has 5 lines, alternating "INFO …" / "ERROR …" so the
/// `ERROR` regex selects half.
pub fn build_fixture(date_yyyymmdd: &str, hours: &[&str]) -> Vec<StagedObject> {
    let mut out = Vec::new();
    for hour in hours {
        let prefix = format!("logs/dt={date_yyyymmdd}/hour={hour}");

        let mk_lines = |service: &str, n: usize| -> Vec<String> {
            (0..n)
                .map(|i| {
                    let level = if i % 2 == 0 { "INFO" } else { "ERROR" };
                    format!(
                        r#"{{"service":"{service}","hour":"{hour}","seq":{i},"level":"{level}","msg":"{level} from {service} #{i}"}}"#,
                    )
                })
                .collect()
        };

        out.push(StagedObject {
            key: format!("{prefix}/service-a-001.json.zst"),
            lines: mk_lines("a", 6),
            encoding: Encoding::Zstd,
        });
        out.push(StagedObject {
            key: format!("{prefix}/service-a-002.json.gz"),
            lines: mk_lines("a2", 4),
            encoding: Encoding::Gzip,
        });
        out.push(StagedObject {
            key: format!("{prefix}/service-b-001.json"),
            lines: mk_lines("b", 6),
            encoding: Encoding::Plain,
        });
        // Filtered-out object (regex requires `service-.*\.(json|json\.gz|json\.zst)$`)
        out.push(StagedObject {
            key: format!("{prefix}/skip-me.txt"),
            lines: vec![r#"{"msg":"ERROR but in skipped file"}"#.into()],
            encoding: Encoding::Plain,
        });
    }
    out
}

/// All lines that should match a given regex across the included objects only.
pub fn expected_matches(staged: &[StagedObject], pattern: &str) -> Vec<String> {
    let re = regex::Regex::new(pattern).unwrap();
    let included = regex::Regex::new(r"service-.*\.(json|json\.gz|json\.zst)$").unwrap();
    let mut out = Vec::new();
    for obj in staged {
        if !included.is_match(&obj.key) {
            continue;
        }
        for line in &obj.lines {
            if re.is_match(line) {
                out.push(line.clone());
            }
        }
    }
    out
}

/// Upload all staged objects to a bucket. Bucket must already exist.
pub async fn seed_bucket(s3: &S3Client, bucket: &str, staged: &[StagedObject]) -> Result<()> {
    for obj in staged {
        let body = obj.body()?;
        s3.put_object()
            .bucket(bucket)
            .key(&obj.key)
            .body(ByteStream::from(body))
            .send()
            .await?;
    }
    Ok(())
}
