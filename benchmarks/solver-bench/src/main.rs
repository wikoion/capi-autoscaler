//! Offline solver benchmark: RQ2 (scaling factorial) and RQ3 (beam vs CP-SAT
//! budget ladder) without a cluster. See benchmarks/Makefile solver-bench-*.

mod arms;
mod catalog;
mod csvout;
mod generator;
mod runner;

use std::path::PathBuf;
use std::process::exit;

use anyhow::{Context, Result, bail};

use arms::Arm;
use generator::{Profile, SlotsLevel};
use runner::{Cell, num_workers, run_cell, run_rq2, run_rq3, run_rq4};

const USAGE: &str = "usage:
  solver-bench rq2 --out <path> [--reps 3] [--budget 30] [--max-attempts 5]
  solver-bench rq3 --out <path> [--reps 3]
  solver-bench rq4 --out <path> [--reps 3]
  solver-bench single --profile <p> --pods <n> --mds <5|10|15> --slots <low|med|high> --arm <prod|beam|cpsat-N>";

fn main() {
    if let Err(e) = run() {
        eprintln!("error: {e:#}");
        eprintln!("{USAGE}");
        exit(1);
    }
}

struct Args {
    flags: std::collections::HashMap<String, String>,
}

impl Args {
    fn parse(args: &[String]) -> Result<Self> {
        let mut flags = std::collections::HashMap::new();
        let mut i = 0;
        while i < args.len() {
            let key = args[i].strip_prefix("--").context("expected --flag")?;
            let val = args.get(i + 1).with_context(|| format!("--{key} needs a value"))?;
            flags.insert(key.to_string(), val.clone());
            i += 2;
        }
        Ok(Self { flags })
    }

    fn get(&self, key: &str) -> Option<&str> {
        self.flags.get(key).map(|s| s.as_str())
    }

    fn get_i32(&self, key: &str, default: i32) -> Result<i32> {
        match self.get(key) {
            Some(v) => v.parse().with_context(|| format!("--{key} must be an integer")),
            None => Ok(default),
        }
    }

    fn require(&self, key: &str) -> Result<&str> {
        self.get(key).with_context(|| format!("--{key} is required"))
    }
}

fn run() -> Result<()> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some((cmd, rest)) = argv.split_first() else { bail!("no subcommand") };
    let args = Args::parse(rest)?;

    match cmd.as_str() {
        "rq2" => {
            let out = PathBuf::from(args.require("out")?);
            run_rq2(
                &out,
                args.get_i32("reps", 3)?,
                args.get_i32("budget", 30)?,
                args.get_i32("max-attempts", 5)?,
            )
        }
        "rq3" => {
            let out = PathBuf::from(args.require("out")?);
            run_rq3(&out, args.get_i32("reps", 3)?)
        }
        "rq4" => {
            let out = PathBuf::from(args.require("out")?);
            run_rq4(&out, args.get_i32("reps", 3)?)
        }
        "single" => {
            let profile = Profile::parse(args.require("profile")?)
                .context("--profile must be unconstrained|light-constrained|heavy-constrained")?;
            let slots = SlotsLevel::parse(args.require("slots")?)
                .context("--slots must be low|med|high")?;
            let arm = Arm::parse(
                args.require("arm")?,
                args.get_i32("budget", 30)?,
                args.get_i32("max-attempts", 5)?,
            )
            .context("--arm must be prod|beam|cpsat-N")?;
            let cell = Cell {
                profile,
                pod_count: args.get_i32("pods", 100)?,
                md_count: args.get_i32("mds", 15)? as usize,
                slots,
                arm,
                rep: 1,
            };
            let row = run_cell("single", &cell, num_workers())?;
            println!("{}", csvout::HEADER.join(","));
            println!("{}", row.join(","));
            Ok(())
        }
        other => bail!("unknown subcommand {other}"),
    }
}
