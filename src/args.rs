pub use colorous::Gradient;

use std::{net::SocketAddr, num::NonZeroUsize, path::PathBuf, str::FromStr};
use structopt::StructOpt;
use timely::{CommunicationConfig, WorkerConfig};

/// Tools for profiling and visualizing Timely Dataflow & Differential Dataflow Programs
///
/// Set the `TIMELY_WORKER_LOG_ADDR` environmental variable to `127.0.0.1:51317` (or whatever
/// address you customized it to using `--listen`) to listen for Timely Dataflow computations
/// and the `DIFFERENTIAL_LOG_ADDR` variable to gather data on Differential Dataflow computations.
/// Set `--connections` to the number of timely workers that the target computation is using.
///
// TODO: Better docs
// TODO: Number of workers
// TODO: Save logs to file
// TODO: Process logs from file
// TODO: Progress logging
// TODO: Build info in help message
// TODO: Reachability logging
// TODO: Disable timeline events
// TODO: This is complex enough to where it may need an
//       actual config file
#[derive(Debug, Clone, StructOpt)]
#[structopt(rename_all = "kebab-case")]
pub struct Args {
    /// The number of ddshow workers to run
    #[structopt(short = "w", long, default_value = "1")]
    pub workers: NonZeroUsize,

    /// The number of timely workers running in the target computation
    #[structopt(short = "c", long = "connections", default_value = "1")]
    pub timely_connections: NonZeroUsize,

    /// The address to listen for Timely Dataflow log messages from
    #[structopt(long = "address", default_value = "127.0.0.1:51317")]
    pub timely_address: SocketAddr,

    /// Whether or not Differential Dataflow logs should be read from
    #[structopt(short = "d", long = "differential")]
    pub differential_enabled: bool,

    /// The address to listen for Differential Dataflow log messages from
    // FIXME: `requires("differential")` makes clap panic
    #[structopt(long, default_value = "127.0.0.1:51318")]
    pub differential_address: SocketAddr,

    /// Whether or not Timely Dataflow progress logs should be read from
    #[structopt(short = "p", long = "progress")]
    pub progress_enabled: bool,

    /// The address to listen for Timely Dataflow progress messages from
    // FIXME: `requires("progress")` makes clap panic
    #[structopt(long, default_value = "127.0.0.1:51319")]
    pub progress_address: SocketAddr,

    /// The color palette to use for the generated graphs
    #[structopt(
        long,
        parse(try_from_str = gradient_from_str),
        possible_values = ACCEPTED_GRADIENTS,
        default_value = "inferno",
    )]
    pub palette: Gradient,

    /// The directory to generate artifacts in
    #[structopt(long, default_value = "dataflow-graph")]
    pub output_dir: PathBuf,

    /// The path to dump the json data to
    ///
    /// The format is currently unstable, so don't depend on it too hard
    #[structopt(long)]
    pub dump_json: Option<PathBuf>,

    /// The folder to save the target process's logs to
    #[structopt(long)]
    pub save_logs: Option<PathBuf>,

    /// The directory to replay a recorded set of logs from
    #[structopt(
        long,
        conflicts_with_all(&["save-logs", "connections", "address", "differential-address", "progress-address"]),
    )]
    pub replay_logs: Option<PathBuf>,

    /// The file to output a text report to
    #[structopt(long, default_value = "report.txt")]
    pub report_file: PathBuf,

    /// Disables text report generation
    #[structopt(long, conflicts_with("report-file"))]
    pub no_report_file: bool,

    /// The coloring to use for terminal output
    #[structopt(long, default_value = "auto", possible_values = &["auto", "always", "never"])]
    pub color: TerminalColor,

    /// Enable profiling for ddshow's internal dataflow
    #[structopt(long)]
    pub dataflow_profiling: bool,

    /// Disables dataflow timeline analysis, can vastly improve performance
    /// and memory usage on very large target dataflows
    #[structopt(long)]
    pub disable_timeline: bool,

    #[structopt(
        long,
        default_value = "abomonation",
        possible_values = &["abomonation", "rkyv"],
    )]
    pub stream_encoding: StreamEncoding,

    /// The time between updating the report file in seconds
    #[structopt(long, conflicts_with("no-report-file"), hidden(true))]
    pub report_update_duration: Option<u8>,

    /// Disables ddshow's terminal output
    #[structopt(long, short = "q", hidden(true))]
    pub quiet: bool,
}

impl Args {
    pub fn timely_config(&self) -> (CommunicationConfig, WorkerConfig) {
        let communication = if self.workers.get() == 1 {
            CommunicationConfig::Thread
        } else {
            CommunicationConfig::Process(self.workers.get())
        };
        let worker_config = WorkerConfig::default();

        // TODO: Implement `Debug` for `timely::Config`
        tracing::trace!("created timely config");

        (communication, worker_config)
    }

    /// Returns `true` if the program is replaying logs from a file
    pub const fn is_file_sourced(&self) -> bool {
        self.replay_logs.is_some()
    }
}

impl Default for Args {
    fn default() -> Self {
        // Safety: One isn't zero
        const ONE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(1) };

        Self {
            workers: ONE,
            timely_connections: ONE,
            timely_address: "127.0.0.1:51317".parse().unwrap(),
            differential_enabled: false,
            differential_address: "127.0.0.1:51318".parse().unwrap(),
            progress_enabled: false,
            progress_address: "127.0.0.1:51319".parse().unwrap(),
            palette: colorous::INFERNO,
            output_dir: PathBuf::from("dataflow-graph"),
            dump_json: None,
            save_logs: None,
            replay_logs: None,
            report_file: PathBuf::from("report.txt"),
            no_report_file: false,
            color: TerminalColor::Auto,
            dataflow_profiling: false,
            disable_timeline: false,
            stream_encoding: StreamEncoding::Abomonation,
            report_update_duration: None,
            quiet: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum StreamEncoding {
    Abomonation,
    Rkyv,
}

impl FromStr for StreamEncoding {
    type Err = String;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        let lowercase = string.to_lowercase();
        match lowercase.as_str() {
            "abomonation" => Ok(Self::Abomonation),
            "rkyv" => Ok(Self::Rkyv),
            _ => Err(format!(
                "invalid terminal color {:?}, only `rkyv` and `abomonation` are supported",
                string,
            )),
        }
    }
}

impl Default for StreamEncoding {
    fn default() -> Self {
        Self::Abomonation
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TerminalColor {
    Auto,
    Always,
    Never,
}

impl TerminalColor {
    /// Returns `true` if the terminal_color is [`Auto`].
    pub const fn is_auto(&self) -> bool {
        matches!(self, Self::Auto)
    }

    /// Returns `true` if the terminal_color is [`Always`].
    pub const fn is_always(&self) -> bool {
        matches!(self, Self::Always)
    }

    // /// Returns `true` if the terminal_color is [`Never`].
    // pub const fn is_never(&self) -> bool {
    //     matches!(self, Self::Never)
    // }
}

impl FromStr for TerminalColor {
    type Err = String;

    fn from_str(string: &str) -> Result<Self, Self::Err> {
        let lowercase = string.to_lowercase();
        match lowercase.as_str() {
            "auto" => Ok(Self::Auto),
            "always" => Ok(Self::Always),
            "never" => Ok(Self::Never),
            _ => Err(format!(
                "invalid terminal color {:?}, only `auto`, `always` and `never` are supported",
                string,
            )),
        }
    }
}

impl Default for TerminalColor {
    fn default() -> Self {
        Self::Auto
    }
}

macro_rules! parse_gradient {
    ($($lower:literal => $gradient:ident),* $(,)?) => {
        fn gradient_from_str(src: &str) -> Result<Gradient, String> {
            let gradient = src.to_lowercase();

            let gradient = match gradient.as_str() {
                $(
                    $lower => colorous::$gradient,
                )*

                _ => return Err(format!("unrecognized gradient '{}'", src)),
            };

            Ok(gradient)
        }

        // TODO: Const eval over proc macro
        const ACCEPTED_GRADIENTS: &'static [&str] = &[$($lower),*];
    };
}

parse_gradient! {
    "turbo" => TURBO,
    "viridis" => VIRIDIS,
    "inferno" => INFERNO,
    "magma" => MAGMA,
    "plasma" => PLASMA,
    "cividis" => CIVIDIS,
    "warm" => WARM,
    "cool" => COOL,
    "cubehelix" => CUBEHELIX,
    "blue-green" => BLUE_GREEN,
    "blue-purple" => BLUE_PURPLE,
    "green-blue" => GREEN_BLUE,
    "orange-red" => ORANGE_RED,
    "purple-blue-green" => PURPLE_BLUE_GREEN,
    "purple-blue" => PURPLE_BLUE,
    "purple-red" => PURPLE_RED,
    "red-purple" => RED_PURPLE,
    "yellow-green-blue" => YELLOW_GREEN_BLUE,
    "yellow-green" => YELLOW_GREEN,
    "yellow-orange-brown" => YELLOW_ORANGE_BROWN,
    "yellow-orange-red" => YELLOW_ORANGE_RED,
}

// #[derive(Debug, Clone, Copy)]
// pub struct ThreadedGradient(Gradient);
//
// impl Deref for ThreadedGradient {
//     type Target = Gradient;
//
//     fn deref(&self) -> &Self::Target {
//         &self.0
//     }
// }
//
// impl Default for ThreadedGradient {
//     fn default() -> Self {
//         Self(colorous::INFERNO)
//     }
// }
//
// unsafe impl Send for ThreadedGradient {}
// unsafe impl Sync for ThreadedGradient {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Output {
    Stdout,
    Stderr,
    Quiet,
}

impl FromStr for Output {
    type Err = String;

    fn from_str(src: &str) -> Result<Self, Self::Err> {
        let string = src.to_owned();

        match string.to_lowercase().as_str() {
            "stdout" => Ok(Self::Stdout),
            "stderr" => Ok(Self::Stderr),
            "quiet" => Ok(Self::Quiet),

            _ => Err(format!("invalid output type: {}", src)),
        }
    }
}
