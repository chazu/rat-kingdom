//! Rat name generation for spawned agents.

pub const RAT_NAMES: [&str; 96] = [
    // Rat/rodent characters & cute critter names.
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
    // Cheeses.
    "Gouda",
    "Brie",
    "Cheddar",
    "Stilton",
    "Colby",
    // Mythic & borrowed rat names.
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
    // More cute critter & action names.
    "Nubbin",
    "Crumb",
    "Morsel",
    "Snuffle",
    "Wriggle",
    "Scritch",
    "Nimble",
    "Patter",
    "Scuttle",
    "Nudge",
    "Bramble",
    "Thistle",
    "Clover",
    "Acorn",
    "Hazel",
    "Pipsqueak",
    "Widget",
    "Gadget",
    "Biscuit",
    "Pumpernickel",
    "Marbles",
    "Peanut",
    "Waffle",
    "Noodle",
    "Mochi",
    "Pretzel",
    // More cheeses.
    "Gruyere",
    "Havarti",
    "Muenster",
    "Provolone",
    "Camembert",
    "Manchego",
    "Roquefort",
    "Parmesan",
    "Asiago",
    "Gorgonzola",
    "Wensleydale",
    "Edam",
    "Jarlsberg",
    "Emmental",
    // Redwall.
    "Matthias",
    "Gonff",
    "Martin",
    "Cornflower",
    "Basil",
    "Cheesethief",
    "Methuselah",
    "Warbeak",
    "Mattimeo",
    "Cluny",
    "Deyna",
    "Triss",
    // Ratatouille.
    "Emile",
    "Django",
    "Gusteau",
    "Linguini",
];

/// Pick the first rat name not in `taken`.
///
/// Once the whole [`RAT_NAMES`] pool is exhausted, generate a still-cute,
/// still-unique name by cycling the pool with an incrementing generation
/// suffix — `Whisker-2`, `Nibbles-2`, … then `Whisker-3`, and so on. Names
/// are never recycled (they are identity keys), and every result stays
/// filesystem/branch-safe (ASCII alphanumeric + dash).
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
    let mut generation = 2;
    loop {
        for name in RAT_NAMES {
            let candidate = format!("{name}-{generation}");
            if !taken.contains(candidate.as_str()) {
                return candidate;
            }
        }
        generation += 1;
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
    fn falls_back_to_cute_generation_suffix() {
        // Whole base pool taken -> cycle it with a generation suffix.
        let all: Vec<&str> = RAT_NAMES.to_vec();
        assert_eq!(next_name(all.iter().copied()), "Whisker-2");

        // First -2 name taken too -> next base name at that generation.
        let mut taken = all.clone();
        taken.push("Whisker-2");
        assert_eq!(next_name(taken.iter().copied()), "Nibbles-2");

        // Whole -2 generation exhausted -> roll over to -3.
        let mut taken = all.clone();
        let gen2: Vec<String> = RAT_NAMES.iter().map(|n| format!("{n}-2")).collect();
        taken.extend(gen2.iter().map(String::as_str));
        assert_eq!(next_name(taken.iter().copied()), "Whisker-3");
    }

    #[test]
    fn generated_names_are_filesystem_safe() {
        for name in RAT_NAMES {
            assert!(
                name.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
                "{name} is not ASCII alphanumeric + dash"
            );
        }
    }
}
