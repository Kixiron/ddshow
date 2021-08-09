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
use ddshow_types::{OperatorAddr, OperatorId};
use std::{
    cmp::Reverse,
    collections::HashMap,
    fmt::{self, Display},
    fs::{self, File},
    io::Write,
    time::Duration,
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

        program_overview(args, data, &mut file)?;
        worker_stats(args, data, &mut file)?;
        operator_stats(
            args,
            data,
            &mut file,
            name_lookup,
            addr_lookup,
            agg_operator_stats,
            agg_arrangement_stats,
        )?;

        if args.differential_enabled {
            arrangement_stats(
                &mut file,
                data,
                name_lookup,
                addr_lookup,
                agg_operator_stats,
                agg_arrangement_stats,
            )?;
        } else {
            tracing::debug!("differential logging is disabled, skipping arrangement stats table");
        }

        operator_tree(
            &mut file,
            data,
            name_lookup,
            addr_lookup,
            agg_operator_stats,
        )?;
    } else {
        tracing::debug!("report files are disabled, skipping generation");
    }

    Ok(())
}

fn program_overview(args: &Args, data: &DataflowData, file: &mut File) -> Result<()> {
    tracing::debug!("generating program overview table");

    let mut table = Table::new();

    table
        .set_header(&["Program Overview", ""])
        .add_row(IntoIterator::into_iter([
            Cell::new("Workers"),
            Cell::new(data.workers.len()),
        ]))
        .add_row(IntoIterator::into_iter([
            Cell::new("Dataflows"),
            Cell::new(data.dataflows.len()),
        ]))
        .add_row(IntoIterator::into_iter([
            Cell::new("Operators"),
            Cell::new(data.operators.len()),
        ]))
        .add_row(IntoIterator::into_iter([
            Cell::new("Subgraphs"),
            Cell::new(data.subgraphs.len()),
        ]))
        .add_row(IntoIterator::into_iter([
            Cell::new("Channels"),
            Cell::new(data.channels.len()),
        ]));

    if args.differential_enabled {
        table.add_row(IntoIterator::into_iter([
            Cell::new("Arrangements"),
            Cell::new(data.arrangement_ids.len()),
        ]));
    }

    let total_runtime = data
        .total_runtime
        .iter()
        .map(|&(_, (start, end))| {
            end.checked_sub(start)
                .unwrap_or_else(|| Duration::from_secs(0))
        })
        .sum::<Duration>()
        .checked_div(data.total_runtime.len() as u32)
        .unwrap_or_else(|| Duration::from_secs(0));

    table.add_row(IntoIterator::into_iter([
        Cell::new("Total Runtime"),
        Cell::new(format!("{:#?}", total_runtime)),
    ]));

    writeln!(file, "{}\n", table).context("failed to write to report file")?;

    Ok(())
}

fn worker_stats(args: &Args, data: &DataflowData, file: &mut File) -> Result<()> {
    tracing::debug!("generating worker stats table");

    let mut table = Table::new();

    let mut headers = vec!["Worker", "Dataflows", "Operators", "Subgraphs", "Channels"];
    if args.differential_enabled {
        headers.push("Arrangements");
    }
    headers.extend(["Runtime"].iter());

    table.set_header(&headers);

    for &worker in data.workers.iter() {
        let mut row = Vec::with_capacity(8);

        row.extend(IntoIterator::into_iter([
            Cell::new(format!("Worker {}", worker.into_inner())),
            Cell::new(data.dataflows.len()),
            Cell::new(data.operators.len()),
            Cell::new(data.subgraphs.len()),
            Cell::new(data.channels.len()),
        ]));

        if args.differential_enabled {
            let arrangements = data
                .arrangement_ids
                .iter()
                .filter(|&&(werker, _)| worker == werker)
                .count();

            row.push(Cell::new(arrangements));
        }

        let total_runtime = data
            .total_runtime
            .iter()
            .filter_map(|&(werker, (start, end))| {
                (worker == werker).then(|| {
                    end.checked_sub(start)
                        .unwrap_or_else(|| Duration::from_secs(0))
                })
            })
            .sum::<Duration>()
            .checked_div(data.total_runtime.len() as u32)
            .unwrap_or_else(|| Duration::from_secs(0));

        row.push(Cell::new(format!("{:#?}", total_runtime)));

        table.add_row(row.drain(..));
    }

    writeln!(file, "Per-Worker Statistics\n{}\n", table)
        .context("failed to write to report file")?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn operator_stats(
    args: &Args,
    data: &DataflowData,
    file: &mut File,
    name_lookup: &HashMap<OpKey, &str, XXHasher>,
    addr_lookup: &HashMap<OpKey, &OperatorAddr, XXHasher>,
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
                let addr = data
                    .workers
                    .iter()
                    .find_map(|&worker| addr_lookup.get(&(worker, operator)));
                let name: Option<&str> = data
                    .workers
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
    data: &DataflowData,
    name_lookup: &HashMap<OpKey, &str, XXHasher>,
    addr_lookup: &HashMap<OpKey, &OperatorAddr, XXHasher>,
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
                let addr = data
                    .workers
                    .iter()
                    .find_map(|&worker| addr_lookup.get(&(worker, operator)));
                let name: Option<&str> = data
                    .workers
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
    data: &DataflowData,
    name_lookup: &HashMap<OpKey, &str, XXHasher>,
    addr_lookup: &HashMap<OpKey, &OperatorAddr, XXHasher>,
    agg_operator_stats: &HashMap<OperatorId, &Summation, XXHasher>,
) -> Result<()> {
    tracing::debug!("generating operator tree");

    let mut tree = Tree::new(|writer, _, (total, name, addr)| {
        writeln!(writer, "{:#?}, {}, {}", total, name, addr)
    });

    for (&operator, &stats) in agg_operator_stats.iter() {
        let addr = *data
            .workers
            .iter()
            .find_map(|&worker| addr_lookup.get(&(worker, operator)))
            .expect("missing operator addr");
        let name = *data
            .workers
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
