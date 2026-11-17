//! Std-only CSV writer, flushed per row so a killed run keeps partial data.

use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::Result;

pub const HEADER: [&str; 27] = [
    "sweep", "arm", "budget_secs", "profile", "pod_count", "md_count", "slots_level",
    "rep", "instance_id", "num_pods", "num_mds", "total_slots", "num_workers",
    "success", "status", "objective", "solve_time_secs", "wall_time_secs",
    "unplaced_pods", "beam_used", "beam_fallback_used", "attempts_run",
    "winning_attempt", "nodes_provisioned", "cost_per_hour", "best_objective_bound",
    "optimality_gap",
];

pub struct CsvWriter {
    out: BufWriter<File>,
}

impl CsvWriter {
    pub fn create(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut out = BufWriter::new(File::create(path)?);
        writeln!(out, "{}", HEADER.join(","))?;
        out.flush()?;
        Ok(Self { out })
    }

    pub fn write_row(&mut self, fields: &[String]) -> Result<()> {
        assert_eq!(fields.len(), HEADER.len(), "row width mismatch");
        // No field we emit contains a comma or quote; quote defensively anyway
        let line: Vec<String> = fields
            .iter()
            .map(|f| {
                if f.contains(',') || f.contains('"') {
                    format!("\"{}\"", f.replace('"', "\"\""))
                } else {
                    f.clone()
                }
            })
            .collect();
        writeln!(self.out, "{}", line.join(","))?;
        self.out.flush()?;
        Ok(())
    }
}
