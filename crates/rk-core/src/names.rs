//! Rat name generation for spawned agents.

pub const RAT_NAMES: [&str; 40] = [
    "Whisker",
    "Nibbles",
    "Scurry",
    "Pip",
    "Templeton",
    "Rizzo",
    "Splinter",
    "Remy",
    "Gnaw",
    "Squeak",
    "Bristle",
    "Twitch",
    "Scamper",
    "Fidget",
    "Munch",
    "Gouda",
    "Brie",
    "Cheddar",
    "Stilton",
    "Colby",
    "Ratatosk",
    "Nezumi",
    "Peppercorn",
    "Sable",
    "Cinder",
    "Ash",
    "Sooty",
    "Dusty",
    "Pockets",
    "Burrow",
    "Tunnel",
    "Rummage",
    "Scrounge",
    "Filch",
    "Swipe",
    "Dart",
    "Skitter",
    "Vole",
    "Shrew",
    "Tails",
];

/// Pick the first rat name not in `taken`; falls back to `rat-N`.
pub fn next_name<'a, I>(taken: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let taken: std::collections::HashSet<&str> = taken.into_iter().collect();
    for name in RAT_NAMES {
        if !taken.contains(name) {
            return name.to_string();
        }
    }
    let mut n = 1;
    loop {
        let name = format!("rat-{n}");
        if !taken.contains(name.as_str()) {
            return name;
        }
        n += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_first_free_name() {
        assert_eq!(next_name([]), "Whisker");
        assert_eq!(next_name(["Whisker"]), "Nibbles");
    }

    #[test]
    fn falls_back_to_numbered() {
        let all: Vec<&str> = RAT_NAMES.to_vec();
        assert_eq!(next_name(all.iter().copied()), "rat-1");
    }
}
