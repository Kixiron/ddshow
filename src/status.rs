#![allow(dead_code)]

use anyhow::{Context, Result};
use crossterm::{
    cursor::{MoveDown, MoveToColumn, RestorePosition, SavePosition, Show},
    queue,
    style::Print,
    terminal::{Clear, ClearType},
};
use std::{
    io::{self, Stdout, Write},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread::{self, Builder, JoinHandle},
    time::Duration,
};

#[derive(Debug, Default)]
pub struct StatusSources {
    timely_events_read: Box<[AtomicUsize]>,
    differential_events_read: Option<Box<[AtomicUsize]>>,
    progress_events_read: Option<Box<[AtomicUsize]>>,
    timely_events_finished: Box<[AtomicBool]>,
    differential_events_finished: Box<[AtomicBool]>,
    progress_events_finished: Box<[AtomicBool]>,
    produced_data: AtomicUsize,
    is_running: AtomicBool,
}

#[derive(Debug, Default)]
pub struct StatusData {
    workers: usize,
    source_workers: usize,
}

pub struct UserInterface {
    stdout: Stdout,
    tick_rate: Duration,
    sources: Arc<StatusSources>,
    status_data: StatusData,
}

impl UserInterface {
    pub fn new(
        tick_rate: Duration,
        sources: Arc<StatusSources>,
        status_data: StatusData,
    ) -> Result<Self> {
        Ok(Self {
            stdout: io::stdout(),
            tick_rate,
            sources,
            status_data,
        })
    }

    pub fn spawn(self) -> JoinHandle<Result<()>> {
        Builder::new()
            .name("ddshow-tui-worker".to_owned())
            .spawn(move || self.render_tui())
            .expect("the thread name is valid")
    }

    pub fn render_tui(mut self) -> Result<()> {
        let mut tick = 0;

        let timely_sources = self.sources.timely_events_read.len();
        debug_assert_eq!(timely_sources, self.sources.timely_events_finished.len());

        queue!(self.stdout, SavePosition).context("failed to queue setup commands")?;

        loop {
            for (idx, (events, finished)) in self
                .sources
                .timely_events_read
                .iter()
                .zip(self.sources.timely_events_finished.iter())
                .enumerate()
            {
                let total_events = events.load(Ordering::Relaxed);
                let is_finished = finished.load(Ordering::Relaxed);

                let print = if is_finished {
                    Print(format!(
                        "[{}/{}] Finished replaying timely trace of {} events",
                        idx + 1,
                        timely_sources,
                        total_events,
                    ))
                } else {
                    Print(format!(
                        "[{}/{}] {} Replaying timely trace: {} events",
                        idx + 1,
                        timely_sources,
                        SPINNERS[0]
                            .chars()
                            .nth(tick % SPINNERS[0].chars().count())
                            .unwrap(),
                        total_events,
                    ))
                };

                queue!(self.stdout, print, MoveDown(1), MoveToColumn(0))
                    .context("failed to queue timely commands")?;
            }

            // Reset the cursor position
            queue!(self.stdout, RestorePosition).context("failed to queue position reset")?;

            self.stdout.flush().context("failed to flush stdout")?;
            tick += 1;

            thread::sleep(self.tick_rate);

            if !self.sources.is_running.load(Ordering::Relaxed) {
                break;
            }
        }

        queue!(self.stdout, RestorePosition, Clear(ClearType::All), Show)
            .context("failed to queue shutdown commands")?;
        self.stdout.flush().context("failed to flush stdout")?;

        Ok(())
    }
}

const SPINNERS: &[&str] = &[
    "⠁⠂⠄⡀⢀⠠⠐⠈",
    "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏",
    "⠋⠙⠚⠞⠖⠦⠴⠲⠳⠓",
    "⠄⠆⠇⠋⠙⠸⠰⠠⠰⠸⠙⠋⠇⠆",
    "⠋⠙⠚⠒⠂⠂⠒⠲⠴⠦⠖⠒⠐⠐⠒⠓⠋",
    "⢹⢺⢼⣸⣇⡧⡗⡏",
];

#[test]
fn tui_test() {
    let tick_rate = Duration::from_millis(200);
    let sources = Arc::new(StatusSources {
        timely_events_read: vec![
            AtomicUsize::new(100),
            AtomicUsize::new(60),
            AtomicUsize::new(2421),
        ]
        .into_boxed_slice(),
        timely_events_finished: vec![
            AtomicBool::new(false),
            AtomicBool::new(false),
            AtomicBool::new(true),
        ]
        .into_boxed_slice(),
        is_running: AtomicBool::new(true),
        ..Default::default()
    });
    let status_data = StatusData::default();

    println!("this should stay here");
    let user_interface = UserInterface::new(tick_rate, sources, status_data).unwrap();
    user_interface.spawn().join().unwrap().unwrap();
}
