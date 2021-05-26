use vergen::Config;

fn main() {
    vergen::vergen(Config::default()).expect("failed to get build info");
}
