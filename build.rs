use vergen::{vergen, Config, ShaKind};

fn main() {
    let mut config = Config::default();
    *config.build_mut().timestamp_mut() = true;
    *config.git_mut().commit_timestamp_mut() = true;
    *config.git_mut().sha_kind_mut() = ShaKind::Short;

    // Generate the instructions
    vergen(config).expect("Unable to generate cargo keys!");
}
