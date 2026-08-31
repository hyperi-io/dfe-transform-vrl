// Project:   dfe-transform-vrl
// File:      tests/integration/mmdb_fixture.rs
// Purpose:   Build a real MaxMind DB file for the MMDB enrichment tests
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! A minimal MaxMind DB writer, so the MMDB enrichment tests run against a
//! real database file rather than a stub.
//!
//! The format is a big-endian binary search tree, a 16-byte zero separator, a
//! data section, then `\xab\xcd\xefMaxMind.com` and the metadata map. What is
//! written here is exactly what `maxminddb::Reader` parses in production: an
//! IPv4-only database with 32-bit records.

use std::collections::BTreeMap;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};

/// Type codes from the MaxMind DB data-section spec.
const TYPE_STRING: u8 = 2;
const TYPE_UINT16: u8 = 5;
const TYPE_UINT32: u8 = 6;
const TYPE_MAP: u8 = 7;
const TYPE_UINT64: u8 = 9;
const TYPE_ARRAY: u8 = 11;

/// Bytes of the data-section separator, which also offsets every data pointer.
const SEPARATOR: usize = 16;

/// A value the writer can encode into the data section.
#[derive(Clone)]
pub enum MmdbValue {
    Str(String),
    U16(u16),
    U32(u32),
    U64(u64),
    Array(Vec<Self>),
    Map(BTreeMap<String, Self>),
}

impl MmdbValue {
    /// A map built from string keys, the only shape these fixtures need.
    pub fn map(pairs: &[(&str, Self)]) -> Self {
        Self::Map(
            pairs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        )
    }

    /// A string value from a `&str`.
    pub fn text(value: &str) -> Self {
        Self::Str(value.to_string())
    }

    fn encode(&self, out: &mut Vec<u8>) {
        match self {
            Self::Str(s) => {
                write_control(out, TYPE_STRING, s.len());
                out.extend_from_slice(s.as_bytes());
            }
            Self::U16(v) => write_uint(out, TYPE_UINT16, u64::from(*v)),
            Self::U32(v) => write_uint(out, TYPE_UINT32, u64::from(*v)),
            Self::U64(v) => write_uint(out, TYPE_UINT64, *v),
            Self::Array(items) => {
                write_control(out, TYPE_ARRAY, items.len());
                for item in items {
                    item.encode(out);
                }
            }
            Self::Map(pairs) => {
                write_control(out, TYPE_MAP, pairs.len());
                for (key, value) in pairs {
                    write_control(out, TYPE_STRING, key.len());
                    out.extend_from_slice(key.as_bytes());
                    value.encode(out);
                }
            }
        }
    }
}

/// Write a control byte: the type in the top three bits, the size below it.
///
/// A type above 7 uses the extended form, where the top three bits are zero
/// and the following byte carries `type - 7`.
fn write_control(out: &mut Vec<u8>, type_num: u8, size: usize) {
    let (top, extended) = if type_num <= TYPE_MAP {
        (type_num, None)
    } else {
        (0, Some(type_num - TYPE_MAP))
    };

    let mut trailer: Vec<u8> = Vec::new();
    let size_bits = if size < 29 {
        u8::try_from(size).expect("size below 29 fits a byte")
    } else if size < 285 {
        trailer.push(u8::try_from(size - 29).expect("size below 285 fits a byte"));
        29
    } else {
        let v = size - 285;
        trailer.push(u8::try_from((v >> 8) & 0xff).expect("masked"));
        trailer.push(u8::try_from(v & 0xff).expect("masked"));
        30
    };

    out.push((top << 5) | size_bits);
    if let Some(ext) = extended {
        out.push(ext);
    }
    out.extend_from_slice(&trailer);
}

/// Write an unsigned integer with the minimal number of big-endian bytes.
fn write_uint(out: &mut Vec<u8>, type_num: u8, value: u64) {
    let bytes = value.to_be_bytes();
    let first = bytes.iter().position(|b| *b != 0).unwrap_or(bytes.len());
    let payload = &bytes[first..];
    write_control(out, type_num, payload.len());
    out.extend_from_slice(payload);
}

/// One side of a search-tree node.
#[derive(Clone, Copy)]
enum Record {
    /// No data for this branch.
    Empty,
    /// Continue at this node index.
    Node(usize),
    /// Data record at this index.
    Data(usize),
}

/// Builds an IPv4-only MaxMind DB with 32-bit records.
pub struct MmdbBuilder {
    nodes: Vec<[Record; 2]>,
    data: Vec<MmdbValue>,
    database_type: String,
}

impl MmdbBuilder {
    pub fn new(database_type: &str) -> Self {
        Self {
            nodes: vec![[Record::Empty; 2]],
            data: Vec::new(),
            database_type: database_type.to_string(),
        }
    }

    /// Map a network onto a record.
    ///
    /// Panics on an overlapping network, which a fixture should never contain.
    pub fn insert(&mut self, network: Ipv4Addr, prefix_len: u8, value: MmdbValue) -> &mut Self {
        assert!(prefix_len > 0 && prefix_len <= 32, "prefix out of range");
        let ip = u32::from(network);
        let data_index = self.data.len();
        self.data.push(value);

        let mut node = 0usize;
        for depth in 0..prefix_len {
            let bit = usize::try_from((ip >> (31 - depth)) & 1).expect("one bit");
            if depth == prefix_len - 1 {
                self.nodes[node][bit] = Record::Data(data_index);
                return self;
            }
            node = match self.nodes[node][bit] {
                Record::Node(next) => next,
                Record::Empty => {
                    self.nodes.push([Record::Empty; 2]);
                    let next = self.nodes.len() - 1;
                    self.nodes[node][bit] = Record::Node(next);
                    next
                }
                Record::Data(_) => panic!("overlapping network in fixture"),
            };
        }
        self
    }

    /// Serialise the database.
    pub fn build(&self) -> Vec<u8> {
        let node_count = self.nodes.len();

        let mut data_section: Vec<u8> = Vec::new();
        let mut offsets: Vec<usize> = Vec::with_capacity(self.data.len());
        for value in &self.data {
            offsets.push(data_section.len());
            value.encode(&mut data_section);
        }

        let mut out: Vec<u8> = Vec::new();
        for node in &self.nodes {
            for record in node {
                let raw = match record {
                    // A data pointer is offset past the node count and the
                    // separator, which is how the reader tells the two apart.
                    Record::Data(index) => node_count + SEPARATOR + offsets[*index],
                    Record::Node(next) => *next,
                    Record::Empty => node_count,
                };
                out.extend_from_slice(
                    &u32::try_from(raw)
                        .expect("fixture is far below 2^32")
                        .to_be_bytes(),
                );
            }
        }

        out.extend_from_slice(&[0u8; SEPARATOR]);
        out.extend_from_slice(&data_section);
        out.extend_from_slice(b"\xab\xcd\xefMaxMind.com");

        let mut description = BTreeMap::new();
        description.insert(
            "en".to_string(),
            MmdbValue::text("dfe-transform-vrl test fixture"),
        );
        MmdbValue::map(&[
            ("binary_format_major_version", MmdbValue::U16(2)),
            ("binary_format_minor_version", MmdbValue::U16(0)),
            ("build_epoch", MmdbValue::U64(1_700_000_000)),
            ("database_type", MmdbValue::text(&self.database_type)),
            ("description", MmdbValue::Map(description)),
            ("ip_version", MmdbValue::U16(4)),
            ("languages", MmdbValue::Array(vec![MmdbValue::text("en")])),
            (
                "node_count",
                MmdbValue::U32(u32::try_from(node_count).expect("fixture node count fits u32")),
            ),
            ("record_size", MmdbValue::U16(32)),
        ])
        .encode(&mut out);

        out
    }

    /// Write the database into `dir` and return its path.
    pub fn write(&self, dir: &Path, name: &str) -> PathBuf {
        let path = dir.join(name);
        std::fs::write(&path, self.build()).expect("fixture mmdb is writable");
        path
    }
}

/// The ASN-shaped fixture the enrichment tests share.
///
/// `1.128.0.0/16` and `12.81.92.0/24` carry records; anything else misses.
pub fn asn_fixture(dir: &Path) -> PathBuf {
    let mut builder = MmdbBuilder::new("DFE-Test-ASN");
    builder
        .insert(
            Ipv4Addr::new(1, 128, 0, 0),
            16,
            MmdbValue::map(&[
                ("autonomous_system_number", MmdbValue::U32(1221)),
                (
                    "autonomous_system_organization",
                    MmdbValue::text("Telstra Pty Ltd"),
                ),
            ]),
        )
        .insert(
            Ipv4Addr::new(12, 81, 92, 0),
            24,
            MmdbValue::map(&[
                ("autonomous_system_number", MmdbValue::U32(7018)),
                ("autonomous_system_organization", MmdbValue::text("AT&T")),
            ]),
        );
    builder.write(dir, "asn-test.mmdb")
}
