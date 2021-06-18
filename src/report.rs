use crate::{args::Args, dataflow::DataflowData};
use anyhow::{Context, Result};
use comfy_table::{presets::UTF8_FULL, Cell, ColumnConstraint, Row, Table as InnerTable};
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
    name_lookup: &HashMap<(WorkerId, OperatorId), String>,
    addr_lookup: &HashMap<(WorkerId, OperatorId), OperatorAddr>,
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

        let all_workers: HashSet<_> = data
            .worker_stats
            .iter()
            .flatten()
            .map(|&(worker, _)| worker)
            .collect();

        program_overview(args, data, &mut file)?;
        worker_stats(args, data, &mut file)?;
        operator_stats(
            args,
            data,
            &mut file,
            &name_lookup,
            &addr_lookup,
            &all_workers,
        )?;

        let mut table = Table::new();
        table.set_header(vec![
            "Operator Address",
            "Channel Id",
            "Produced Messages",
            "Consumed Messages",
            "Produced Capability Updates",
            "Consumed Capability Updates",
        ]);

        for (addr, info) in data.channel_progress.iter() {
            table.add_row(vec![
                Cell::new(addr),
                Cell::new(info.channel_id),
                Cell::new(info.produced.messages),
                Cell::new(info.consumed.messages),
                Cell::new(info.produced.capability_updates),
                Cell::new(info.consumed.capability_updates),
            ]);
        }

        writeln!(file, "{}\n", table).context("failed to write to report file")?;

        if args.differential_enabled {
            arrangement_stats(data, &mut file, &name_lookup, &addr_lookup, &all_workers)?;
        } else {
            tracing::debug!("differential logging is disabled, skipping arrangement stats table");
        }
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
            .set_header(vec!["Program Overview", ""])
            .add_row(vec![Cell::new("Workers"), Cell::new(stats.workers)])
            .add_row(vec![Cell::new("Dataflows"), Cell::new(stats.dataflows)])
            .add_row(vec![Cell::new("Operators"), Cell::new(stats.operators)])
            .add_row(vec![Cell::new("Subgraphs"), Cell::new(stats.subgraphs)])
            .add_row(vec![Cell::new("Channels"), Cell::new(stats.channels)]);

        if args.differential_enabled {
            table.add_row(vec![
                Cell::new("Arrangements"),
                Cell::new(stats.arrangements),
            ]);
        }

        table
            .add_row(vec![Cell::new("Events"), Cell::new(stats.events)])
            .add_row(vec![
                Cell::new("Total Runtime"),
                Cell::new(format!("{:#?}", stats.runtime)),
            ]);

        writeln!(file, "{}\n", table).context("failed to write to report file")?;
    } else {
        tracing::error!("didn't receive a program stats entry");
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

    table.set_header(headers);

    // Ensure that we received exactly one entry
    debug_assert_eq!(worker_stats.len(), 1);
    // Ensure the vector is sorted
    debug_assert!(worker_stats[0].windows(2).all(|x| x[0] <= x[1]));

    for (worker, stats) in worker_stats.first().expect("the length is equal to one") {
        let mut row = vec![
            Cell::new(format!("Worker {}", worker.into_inner())),
            Cell::new(stats.dataflows),
            Cell::new(stats.operators),
            Cell::new(stats.subgraphs),
            Cell::new(stats.channels),
        ];

        if args.differential_enabled {
            row.push(Cell::new(stats.arrangements));
        }

        row.extend(vec![
            Cell::new(stats.events),
            Cell::new(format!("{:#?}", stats.runtime)),
        ]);

        table.add_row(row);
    }

    writeln!(file, "Per-Worker Statistics\n{}\n", table)
        .context("failed to write to report file")?;

    Ok(())
}

fn operator_stats(
    args: &Args,
    data: &DataflowData,
    file: &mut File,
    name_lookup: &HashMap<(WorkerId, OperatorId), String>,
    addr_lookup: &HashMap<(WorkerId, OperatorId), OperatorAddr>,
    all_workers: &HashSet<WorkerId>,
) -> Result<()> {
    tracing::debug!("generating operator stats table");

    // TODO: Sort within timely
    let mut operators_by_total_runtime = data.aggregated_operator_stats.clone();
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

    table.set_header(headers);

    for (operator, stats, addr, name) in operators_by_total_runtime
        .iter()
        .filter_map(|&(operator, ref stats)| {
            all_workers.iter().find_map(|&worker| {
                addr_lookup
                    .get(&(worker, operator))
                    .map(|addr| (operator, stats, addr))
            })
        })
        .map(|(operator, stats, addr)| {
            let name: Option<&str> = all_workers
                .iter()
                .find_map(|&worker| name_lookup.get(&(worker, operator)).map(|name| &**name));

            (operator, stats, addr, name.unwrap_or("N/A"))
        })
    {
        let arrange = stats.arrangement_size.as_ref().map(|arrange| {
            (
                format!("{}", arrange.max_size),
                format!("{}", arrange.min_size),
                format!("{}", arrange.batches),
            )
        });

        let mut row = vec![
            Cell::new(name),
            Cell::new(operator),
            Cell::new(format!(
                "[{}]",
                addr.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            )),
            Cell::new(format!("{:#?}", stats.total)),
            Cell::new(stats.activations),
            Cell::new(format!("{:#?}", stats.average)),
            Cell::new(format!("{:#?}", stats.max)),
            Cell::new(format!("{:#?}", stats.min)),
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
    data: &DataflowData,
    file: &mut File,
    name_lookup: &HashMap<(WorkerId, OperatorId), String>,
    addr_lookup: &HashMap<(WorkerId, OperatorId), OperatorAddr>,
    all_workers: &HashSet<WorkerId>,
) -> Result<()> {
    tracing::debug!("generating arrangement stats table");

    let mut operators_by_arrangement_size: Vec<_> = data
        .aggregated_operator_stats
        .iter()
        .filter_map(|&(operator, ref stats)| {
            stats
                .arrangement_size
                .map(|arrange| (operator, stats.clone(), arrange))
        })
        .collect();

    operators_by_arrangement_size.sort_unstable_by_key(|(_, _, arrange)| Reverse(arrange.max_size));

    let mut table = Table::new();
    table.set_header(vec![
        "Name",
        "Id",
        "Address",
        "Total Runtime",
        "Max Arrangement Size",
        "Min Arrangement Size",
        "Arrangement Batches",
    ]);

    for (operator, stats, arrange, addr, name) in operators_by_arrangement_size
        .iter()
        .filter_map(|&(operator, ref stats, arrange)| {
            all_workers.iter().find_map(|&worker| {
                addr_lookup
                    .get(&(worker, operator))
                    .map(|addr| (operator, stats, arrange, addr))
            })
        })
        .map(|(operator, stats, arrange, addr)| {
            let name: Option<&str> = all_workers
                .iter()
                .find_map(|&worker| name_lookup.get(&(worker, operator)).map(|name| &**name));

            (operator, stats, arrange, addr, name.unwrap_or("N/A"))
        })
    {
        table.add_row(vec![
            Cell::new(name),
            Cell::new(operator),
            Cell::new(format!(
                "[{}]",
                addr.iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            )),
            Cell::new(format!("{:#?}", stats.total)),
            Cell::new(arrange.max_size),
            Cell::new(arrange.min_size),
            Cell::new(arrange.batches),
        ]);
    }

    writeln!(file, "Operators Ranked by Arrangement Size\n{}\n", table,)
        .context("failed to write to report file")?;

    Ok(())
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

    fn set_header(&mut self, row: Vec<&str>) -> &mut Self {
        self.inner
            .set_constraints(
                row.iter()
                    .map(|header| ColumnConstraint::MinWidth(header.len() as u16)),
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
