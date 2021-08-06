mod tree;

use crate::{
    args::Args,
    dataflow::{
        utils::{OpKey, XXHasher},
        ArrangementStats, DataflowData, Summation,
    },
    report::tree::Tree,
};
use anyhow::{Context, Result};
use comfy_table::{presets::UTF8_FULL, Cell, ColumnConstraint, Row, Table as InnerTable, Width};
use ddshow_types::{OperatorAddr, OperatorId, WorkerId};
use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    fmt::{self, Display},
    fs::{self, File},
    io::Write,
};

pub fn build_report(
    args: &Args,
    data: &DataflowData,
    name_lookup: &HashMap<OpKey, &str, XXHasher>,
    addr_lookup: &HashMap<OpKey, &OperatorAddr, XXHasher>,
    agg_operator_stats: &HashMap<OperatorId, &Summation, XXHasher>,
    agg_arrangement_stats: &HashMap<OperatorId, &ArrangementStats, XXHasher>,
) -> Result<()> {
    if !args.no_report_file {
        // Attempt to create the path up to the report file
        if let Some(parent) = args
            .report_file
            .canonicalize()
            .ok()
            .and_then(|path| path.parent().map(ToOwned::to_owned))
        {
            tracing::debug!(
                "creating parent directory for the report file: {}",
                parent.display(),
            );

            if let Err(err) = fs::create_dir_all(&parent) {
                tracing::error!(
                    parent = %parent.display(),
                    "failed to create parent path for report file: {:?}",
                    err,
                );
            }
        }

        // Create the report file
        tracing::debug!("creating report file: {}", args.report_file.display());
        let mut file = File::create(&args.report_file).context("failed to create report file")?;

        let all_workers: HashSet<_, XXHasher> = data
            .addr_lookup
            .iter()
            .map(|&((worker, _), _)| worker)
            .collect();

        program_overview(args, data, &mut file)?;
        worker_stats(args, data, &mut file)?;
        operator_stats(
            args,
            data,
            &mut file,
            name_lookup,
            addr_lookup,
            &all_workers,
            agg_operator_stats,
            agg_arrangement_stats,
        )?;

        if args.differential_enabled {
            arrangement_stats(
                &mut file,
                name_lookup,
                addr_lookup,
                &all_workers,
                agg_operator_stats,
                agg_arrangement_stats,
            )?;
        } else {
            tracing::debug!("differential logging is disabled, skipping arrangement stats table");
        }

        operator_tree(
            &mut file,
            name_lookup,
            addr_lookup,
            &all_workers,
            agg_operator_stats,
        )?;
    } else {
        tracing::debug!("report files are disabled, skipping generation");
    }

    Ok(())
}

fn program_overview(args: &Args, data: &DataflowData, file: &mut File) -> Result<()> {
    tracing::debug!("generating program overview table");

    if let Some(stats) = data.program_stats.last() {
        let mut table = Table::new();

        table
            .set_header(&["Program Overview", ""])
            .add_row(IntoIterator::into_iter([
                Cell::new("Workers"),
                Cell::new(stats.workers),
            ]))
            .add_row(IntoIterator::into_iter([
                Cell::new("Dataflows"),
                Cell::new(stats.dataflows),
            ]))
            .add_row(IntoIterator::into_iter([
                Cell::new("Operators"),
                Cell::new(stats.operators),
            ]))
            .add_row(IntoIterator::into_iter([
                Cell::new("Subgraphs"),
                Cell::new(stats.subgraphs),
            ]))
            .add_row(IntoIterator::into_iter([
                Cell::new("Channels"),
                Cell::new(stats.channels),
            ]));

        if args.differential_enabled {
            table.add_row(IntoIterator::into_iter([
                Cell::new("Arrangements"),
                Cell::new(stats.arrangements),
            ]));
        }

        table
            .add_row(IntoIterator::into_iter([
                Cell::new("Events"),
                Cell::new(stats.events),
            ]))
            .add_row(IntoIterator::into_iter([
                Cell::new("Total Runtime"),
                Cell::new(format!("{:#?}", stats.runtime)),
            ]));

        writeln!(file, "{}\n", table).context("failed to write to report file")?;
    } else {
        tracing::warn!("didn't receive a program stats entry");

        writeln!(file, "No Program Statistics were received\n")
            .context("failed to write to report file")?;
    }

    Ok(())
}

fn worker_stats(args: &Args, data: &DataflowData, file: &mut File) -> Result<()> {
    tracing::debug!("generating worker stats table");

    let mut table = Table::new();
    let worker_stats = &data.worker_stats;

    let mut headers = vec!["Worker", "Dataflows", "Operators", "Subgraphs", "Channels"];
    if args.differential_enabled {
        headers.push("Arrangements");
    }
    headers.extend(["Events", "Runtime"].iter());

    table.set_header(&headers);

    if let Some(stats) = worker_stats.last() {
        // There should only be one entry
        debug_assert_eq!(worker_stats.len(), 1);

        let mut row = Vec::with_capacity(8);
        for (worker, stats) in stats {
            row.extend(IntoIterator::into_iter([
                Cell::new(format!("Worker {}", worker.into_inner())),
                Cell::new(stats.dataflows),
                Cell::new(stats.operators),
                Cell::new(stats.subgraphs),
                Cell::new(stats.channels),
            ]));

            if args.differential_enabled {
                row.push(Cell::new(stats.arrangements));
            }

            row.extend(vec![
                Cell::new(stats.events),
                Cell::new(format!("{:#?}", stats.runtime)),
            ]);

            table.add_row(row.drain(..));
        }

        writeln!(file, "Per-Worker Statistics\n{}\n", table)
            .context("failed to write to report file")?;
    } else {
        tracing::warn!("didn't receive any worker stats entries");

        writeln!(file, "No Per-Worker Statistics were received\n")
            .context("failed to write to report file")?;
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn operator_stats(
    args: &Args,
    data: &DataflowData,
    file: &mut File,
    name_lookup: &HashMap<OpKey, &str, XXHasher>,
    addr_lookup: &HashMap<OpKey, &OperatorAddr, XXHasher>,
    all_workers: &HashSet<WorkerId, XXHasher>,
    agg_operator_stats: &HashMap<OperatorId, &Summation, XXHasher>,
    agg_arrangement_stats: &HashMap<OperatorId, &ArrangementStats, XXHasher>,
) -> Result<()> {
    tracing::debug!("generating operator stats table");

    // TODO: Sort within timely
    let mut operators_by_total_runtime: Vec<_> = agg_operator_stats.iter().collect();
    operators_by_total_runtime.sort_by_key(|(_operator, stats)| Reverse(stats.total));

    let mut table = Table::new();

    let mut headers = vec![
        "Name",
        "Id",
        "Address",
        "Total Runtime",
        "Activations",
        "Average Activation Time",
        "Max Activation Time",
        "Min Activation Time",
        "Inputs",
        "Outputs",
    ];
    if args.differential_enabled {
        headers.extend(
            [
                "Max Arrangement Size",
                "Min Arrangement Size",
                "Arrangement Batches",
            ]
            .iter(),
        );
    }

    table.set_header(&headers);

    for (operator, stats, addr, name) in
        operators_by_total_runtime
            .iter()
            .map(|&(&operator, &stats)| {
                let addr = all_workers
                    .iter()
                    .find_map(|&worker| addr_lookup.get(&(worker, operator)));
                let name: Option<&str> = all_workers
                    .iter()
                    .find_map(|&worker| name_lookup.get(&(worker, operator)).map(|name| &**name));

                (operator, stats, addr, name.unwrap_or(""))
            })
    {
        let arrange = agg_arrangement_stats.get(&operator).map(|&arrange| {
            (
                format!("{}", arrange.max_size),
                format!("{}", arrange.min_size),
                format!("{}", arrange.batches),
            )
        });

        let (inputs, outputs) = data
            .operator_shapes
            .iter()
            .find_map(|shape| {
                if shape.id == operator {
                    Some((shape.inputs.len(), shape.outputs.len()))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                tracing::warn!("couldn't find operator shape for {}", operator);
                (0, 0)
            });

        let mut row = vec![
            Cell::new(name),
            Cell::new(operator),
            Cell::new(format!(
                "[{}]",
                addr.map_or_else(
                    || String::from("{unknown}"),
                    |addr| addr
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )),
            Cell::new(format!("{:#?}", stats.total)),
            Cell::new(stats.count),
            Cell::new(format!("{:#?}", stats.average)),
            Cell::new(format!("{:#?}", stats.max)),
            Cell::new(format!("{:#?}", stats.min)),
            Cell::new(inputs),
            Cell::new(outputs),
        ];

        if let Some((max, min, batches)) = arrange {
            row.extend(vec![Cell::new(max), Cell::new(min), Cell::new(batches)]);
        }

        table.add_row(row);
    }

    writeln!(file, "Operators Ranked by Total Runtime\n{}\n", table,)
        .context("failed to write to report file")?;

    Ok(())
}

fn arrangement_stats(
    file: &mut File,
    name_lookup: &HashMap<OpKey, &str, XXHasher>,
    addr_lookup: &HashMap<OpKey, &OperatorAddr, XXHasher>,
    all_workers: &HashSet<WorkerId, XXHasher>,
    agg_operator_stats: &HashMap<OperatorId, &Summation, XXHasher>,
    agg_arrangement_stats: &HashMap<OperatorId, &ArrangementStats, XXHasher>,
) -> Result<()> {
    tracing::debug!("generating arrangement stats table");

    let mut operators_by_arrangement_size: Vec<_> = agg_arrangement_stats
        .iter()
        .filter_map(|(&operator, &arrange)| {
            agg_operator_stats
                .get(&operator)
                .map(|&stats| (operator, stats, arrange))
        })
        .collect();

    operators_by_arrangement_size.sort_unstable_by_key(|(_, _, arrange)| Reverse(arrange.max_size));

    let mut table = Table::new();
    table.set_header(&[
        "Name",
        "Id",
        "Address",
        "Total Runtime",
        "Max Arrangement Size",
        "Min Arrangement Size",
        "Arrangement Batches",
    ]);

    for (operator, stats, arrange, addr, name) in
        operators_by_arrangement_size
            .iter()
            .map(|&(operator, ref stats, arrange)| {
                let addr = all_workers
                    .iter()
                    .find_map(|&worker| addr_lookup.get(&(worker, operator)));
                let name: Option<&str> = all_workers
                    .iter()
                    .find_map(|&worker| name_lookup.get(&(worker, operator)).map(|name| &**name));

                (operator, stats, arrange, addr, name.unwrap_or(""))
            })
    {
        table.add_row(IntoIterator::into_iter([
            Cell::new(name),
            Cell::new(operator),
            Cell::new(format!(
                "[{}]",
                addr.map_or_else(
                    || String::from("{unknown}"),
                    |addr| addr
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            )),
            Cell::new(format!("{:#?}", stats.total)),
            Cell::new(arrange.max_size),
            Cell::new(arrange.min_size),
            Cell::new(arrange.batches),
        ]));
    }

    writeln!(file, "Operators Ranked by Arrangement Size\n{}\n", table,)
        .context("failed to write to report file")?;

    Ok(())
}

fn operator_tree(
    file: &mut File,
    name_lookup: &HashMap<OpKey, &str, XXHasher>,
    addr_lookup: &HashMap<OpKey, &OperatorAddr, XXHasher>,
    all_workers: &HashSet<WorkerId, XXHasher>,
    agg_operator_stats: &HashMap<OperatorId, &Summation, XXHasher>,
) -> Result<()> {
    tracing::debug!("generating operator tree");

    let mut tree = Tree::new(|writer, _, (total, name, addr)| {
        writeln!(writer, "{:#?}, {}, {}", total, name, addr)
    });

    for (&operator, &stats) in agg_operator_stats.iter() {
        let addr = *all_workers
            .iter()
            .find_map(|&worker| addr_lookup.get(&(worker, operator)))
            .expect("missing operator addr");
        let name = *all_workers
            .iter()
            .find_map(|&worker| name_lookup.get(&(worker, operator)))
            .expect("missing operator name");

        tree.insert(addr.as_slice(), (stats.total, name, addr));
        // debug_assert_eq!(displaced, None);
    }

    // FIXME: Things aren't actually getting sorted for some reason
    tree.sort_unstable_by(|(_, left), (_, right)| {
        left.map(|&(total, _, _)| Reverse(total))
            .cmp(&right.map(|&(total, _, _)| Reverse(total)))
    });

    write!(file, "Operator Tree\n{}", tree).context("failed to write to report file")
}

struct Table {
    inner: InnerTable,
}

impl Table {
    fn new() -> Self {
        let mut inner = InnerTable::new();
        inner.load_preset(UTF8_FULL);

        Self { inner }
    }

    fn set_header(&mut self, row: &[&str]) -> &mut Self {
        self.inner
            .set_constraints(
                row.iter().map(|header| {
                    ColumnConstraint::LowerBoundary(Width::Fixed(header.len() as u16))
                }),
            )
            .set_header(row);

        self
    }

    fn add_row<T>(&mut self, row: T) -> &mut Self
    where
        T: Into<Row>,
    {
        self.inner.add_row(row);
        self
    }
}

impl Display for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.inner, f)
    }
}
