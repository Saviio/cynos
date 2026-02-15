//! Performance report generation

use crate::utils::{format_duration, format_throughput, BenchResult};
use std::collections::HashMap;

#[derive(Clone)]
pub struct BenchEntry {
    pub name: String,
    pub category: String,
    pub size: Option<usize>,
    pub result: BenchResult,
    pub throughput: Option<f64>,
    pub target: Option<&'static str>,
    pub passed: Option<bool>,
}

pub struct Report {
    entries: Vec<BenchEntry>,
    categories: HashMap<String, Vec<usize>>,
}

impl Report {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            categories: HashMap::new(),
        }
    }

    pub fn add(&mut self, entry: BenchEntry) {
        let idx = self.entries.len();
        self.categories
            .entry(entry.category.clone())
            .or_default()
            .push(idx);
        self.entries.push(entry);
    }

    pub fn add_result(
        &mut self,
        category: &str,
        name: &str,
        size: Option<usize>,
        result: BenchResult,
        throughput: Option<f64>,
    ) {
        self.add(BenchEntry {
            name: name.to_string(),
            category: category.to_string(),
            size,
            result,
            throughput,
            target: None,
            passed: None,
        });
    }

    pub fn add_with_target(
        &mut self,
        category: &str,
        name: &str,
        size: Option<usize>,
        result: BenchResult,
        throughput: Option<f64>,
        target: &'static str,
        passed: bool,
    ) {
        self.add(BenchEntry {
            name: name.to_string(),
            category: category.to_string(),
            size,
            result,
            throughput,
            target: Some(target),
            passed: Some(passed),
        });
    }

    pub fn print_summary(&self) {
        println!("╔══════════════════════════════════════════════════════════════════╗");
        println!("║                      PERFORMANCE SUMMARY                         ║");
        println!("╚══════════════════════════════════════════════════════════════════╝\n");

        // Group by category
        let mut categories: Vec<_> = self.categories.keys().collect();
        categories.sort();

        for category in categories {
            println!("┌─ {} ─", category);
            if let Some(indices) = self.categories.get(category.as_str()) {
                for &idx in indices {
                    let entry = &self.entries[idx];
                    let size_str = entry
                        .size
                        .map(|s| format!(" [{:>6}]", format_size(s)))
                        .unwrap_or_default();

                    let status = match entry.passed {
                        Some(true) => "✓",
                        Some(false) => "✗",
                        None => " ",
                    };

                    let throughput_str = entry
                        .throughput
                        .map(|t| format!(" ({})", format_throughput(t)))
                        .unwrap_or_default();

                    let target_str = entry
                        .target
                        .map(|t| format!(" [target: {}]", t))
                        .unwrap_or_default();

                    println!(
                        "│ {} {:<30}{}: {:>12}{}{}",
                        status,
                        entry.name,
                        size_str,
                        format_duration(entry.result.mean),
                        throughput_str,
                        target_str
                    );
                }
            }
            println!("└─");
            println!();
        }

        // Print pass/fail summary
        let total = self.entries.iter().filter(|e| e.passed.is_some()).count();
        let passed = self
            .entries
            .iter()
            .filter(|e| e.passed == Some(true))
            .count();
        let failed = self
            .entries
            .iter()
            .filter(|e| e.passed == Some(false))
            .count();

        if total > 0 {
            println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
            println!(
                "  Target Checks: {} passed, {} failed, {} total",
                passed, failed, total
            );
            if failed > 0 {
                println!("  Status: SOME TARGETS NOT MET");
            } else {
                println!("  Status: ALL TARGETS MET ✓");
            }
            println!("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
        }
    }
}

fn format_size(size: usize) -> String {
    if size >= 1_000_000 {
        format!("{}M", size / 1_000_000)
    } else if size >= 1_000 {
        format!("{}K", size / 1_000)
    } else {
        format!("{}", size)
    }
}
