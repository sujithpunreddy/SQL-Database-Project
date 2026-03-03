import os
import math
import random
import sqlite3
from datetime import date, timedelta

DB_PATH = "coastal_microplastics.sqlite"

def choose_weighted(options, weights):
    r = random.random() * sum(weights)
    s = 0.0
    for opt, w in zip(options, weights):
        s += w
        if s >= r:
            return opt
    return options[-1]


# density -> ordinal label
def severity_from_density(d):
    if d < 60:
        return "Low"
    if d < 120:
        return "Moderate"
    if d < 220:
        return "High"
    return "Critical"


def main():
    random.seed(42)

    # start fresh
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")

    # schema
    conn.executescript(
        """
        PRAGMA foreign_keys = ON;

        DROP TABLE IF EXISTS beach_daily_summary;
        DROP TABLE IF EXISTS event_volunteers;
        DROP TABLE IF EXISTS cleanup_events;
        DROP TABLE IF EXISTS lab_results;
        DROP TABLE IF EXISTS samples;
        DROP TABLE IF EXISTS volunteers;
        DROP TABLE IF EXISTS beaches;

        CREATE TABLE beaches (
            beach_id INTEGER PRIMARY KEY,
            beach_name TEXT NOT NULL,
            region TEXT NOT NULL CHECK (region IN ('North','South','East','West')),
            beach_type TEXT NOT NULL CHECK (beach_type IN ('Sandy','Rocky','Mixed')),
            access_level TEXT NOT NULL CHECK (access_level IN ('Easy','Moderate','Hard')),
            length_km REAL NOT NULL CHECK (length_km > 0),
            protected_status TEXT NOT NULL CHECK (protected_status IN ('None','Local','National'))
        );

        CREATE TABLE volunteers (
            volunteer_id INTEGER PRIMARY KEY,
            full_name TEXT NOT NULL,
            age INTEGER NOT NULL CHECK (age BETWEEN 16 AND 80),
            experience_level TEXT NOT NULL CHECK (experience_level IN ('New','Intermediate','Experienced')),
            email TEXT,
            phone TEXT
        );

        CREATE TABLE samples (
            sample_id INTEGER PRIMARY KEY,
            beach_id INTEGER NOT NULL,
            collection_date TEXT NOT NULL,
            collection_method TEXT NOT NULL CHECK (collection_method IN ('Transect','Quadrat','Grab','Sieve')),
            polymer_type TEXT,
            plastic_density_mg_m2 REAL NOT NULL CHECK (plastic_density_mg_m2 >= 0),
            severity TEXT NOT NULL CHECK (severity IN ('Low','Moderate','High','Critical')),
            FOREIGN KEY (beach_id) REFERENCES beaches(beach_id)
        );

        CREATE TABLE lab_results (
            lab_result_id INTEGER PRIMARY KEY,
            sample_id INTEGER NOT NULL UNIQUE,
            microplastic_count INTEGER NOT NULL CHECK (microplastic_count >= 0),
            size_class TEXT NOT NULL CHECK (size_class IN ('Small','Medium','Large')),
            analyst_confidence REAL,
            notes TEXT,
            FOREIGN KEY (sample_id) REFERENCES samples(sample_id)
        );

        CREATE TABLE cleanup_events (
            event_id INTEGER PRIMARY KEY,
            beach_id INTEGER NOT NULL,
            event_date TEXT NOT NULL,
            event_type TEXT NOT NULL CHECK (event_type IN ('Cleanup','Awareness','Survey')),
            sponsor TEXT NOT NULL,
            bags_collected INTEGER NOT NULL CHECK (bags_collected >= 0),
            FOREIGN KEY (beach_id) REFERENCES beaches(beach_id)
        );

        CREATE TABLE event_volunteers (
            event_id INTEGER NOT NULL,
            volunteer_id INTEGER NOT NULL,
            hours_worked REAL NOT NULL CHECK (hours_worked >= 0),
            role TEXT NOT NULL CHECK (role IN ('Leader','Helper','DataEntry','Safety')),
            PRIMARY KEY (event_id, volunteer_id),
            FOREIGN KEY (event_id) REFERENCES cleanup_events(event_id) ON DELETE CASCADE,
            FOREIGN KEY (volunteer_id) REFERENCES volunteers(volunteer_id) ON DELETE CASCADE
        );

        CREATE TABLE beach_daily_summary (
            beach_id INTEGER NOT NULL,
            summary_date TEXT NOT NULL,
            sample_count INTEGER NOT NULL CHECK (sample_count >= 0),
            avg_density_mg_m2 REAL NOT NULL CHECK (avg_density_mg_m2 >= 0),
            dominant_severity TEXT NOT NULL CHECK (dominant_severity IN ('Low','Moderate','High','Critical')),
            PRIMARY KEY (beach_id, summary_date),
            FOREIGN KEY (beach_id) REFERENCES beaches(beach_id)
        );
        """
    )

    # beaches
    beach_names = [
        "Blue Cove", "Dune Shore", "Pebble Bay", "Seabrook Bay", "Brightwater Bay",
        "Coral Point", "Windy Strand", "Harbor Sands", "Silver Beach", "Turtle Coast",
        "Wavecrest", "Pine Dunes", "Sunset Reach", "Stormwatch", "Crystal Shore",
        "Lighthouse Bay", "Crescent Beach", "Driftwood Cove", "Otter Bay", "Sapphire Sands",
        "Moonlit Shore", "Cliffside Bay", "Golden Strand", "Seagrass Point", "Mariner's Cove",
        "Palm Beach", "Cedar Coast", "Bluefin Bay", "Dolphin Shore", "Whispering Sands"
    ]
    regions = ["North", "South", "East", "West"]
    beach_types = ["Sandy", "Rocky", "Mixed"]
    access_levels = ["Easy", "Moderate", "Hard"]
    protected = ["None", "Local", "National"]

    beaches = []
    for i in range(1, 31):
        beaches.append(
            (
                i,
                beach_names[i - 1],
                choose_weighted(regions, [0.28, 0.28, 0.22, 0.22]),
                choose_weighted(beach_types, [0.55, 0.20, 0.25]),
                choose_weighted(access_levels, [0.60, 0.30, 0.10]),
                round(max(0.5, random.gauss(3.8, 1.4)), 2),
                choose_weighted(protected, [0.60, 0.25, 0.15]),
            )
        )
    conn.executemany("INSERT INTO beaches VALUES (?,?,?,?,?,?,?)", beaches)

    # volunteers
    first = ["Sujith", "Asha", "Ravi", "Nina", "Omar", "Elena", "Ivy", "Noah", "Liam", "Maya", "Arjun", "Sara"]
    last = ["Reddy", "Patel", "Khan", "Singh", "Brown", "Garcia", "Silva", "Lee", "Chen", "Jones", "Rossi", "Miller"]
    levels = ["New", "Intermediate", "Experienced"]

    volunteers = []
    for vid in range(1, 481):
        name = f"{random.choice(first)} {random.choice(last)}"
        age = int(min(75, max(17, round(random.gauss(29, 10)))))
        exp = choose_weighted(levels, [0.45, 0.35, 0.20])

        # some missing contacts
        email = f"user{vid}@mail.com" if random.random() > 0.05 else None
        phone = f"+44 7{random.randint(100000000, 999999999)}" if random.random() > 0.08 else None

        volunteers.append((vid, name, age, exp, email, phone))

    conn.executemany("INSERT INTO volunteers VALUES (?,?,?,?,?,?)", volunteers)

    # samples (main big table)
    methods = ["Transect", "Quadrat", "Grab", "Sieve"]
    polymers = ["PET", "PE", "PP", "PS", "PVC", "Nylon", "PU", "Other"]

    samples = []
    start_date = date(2024, 1, 1)

    for sid in range(1, 5001):
        beach_id = random.randint(1, 30)
        d = start_date + timedelta(days=random.randint(0, 750))

        density = max(0.0, random.lognormvariate(math.log(140), 0.55))
        density = round(density, 1)

        polymer = random.choice(polymers)

        # some missing values
        if random.random() < 0.04:
            polymer = None

        sev = severity_from_density(density)

        samples.append(
            (
                sid,
                beach_id,
                d.isoformat(),
                random.choice(methods),
                polymer,
                density,
                sev,
            )
        )

    conn.executemany("INSERT INTO samples VALUES (?,?,?,?,?,?,?)", samples)

    # lab_results (1:1 with samples)
    size_classes = ["Small", "Medium", "Large"]
    lab_rows = []
    for sid in range(1, 5001):
        # count roughly linked to density
        base = max(0, int(round(random.gauss(80, 25))))
        extra = int(max(0, random.gauss(0.9 * (sid % 60), 10)))
        count = base + extra

        size_class = choose_weighted(size_classes, [0.55, 0.30, 0.15])

        conf = round(min(1.0, max(0.3, random.gauss(0.83, 0.12))), 2)

        # a few NULL confidence
        if random.random() < 0.028:
            conf = None

        note = None
        if random.random() < 0.03:
            note = "rechecked"

        lab_rows.append((sid, sid, count, size_class, conf, note))

    conn.executemany("INSERT INTO lab_results VALUES (?,?,?,?,?,?)", lab_rows)

    # cleanup events
    event_types = ["Cleanup", "Awareness", "Survey"]
    sponsors = ["CoastalCare", "BlueOcean Club", "SeaWatch", "EcoPulse", "Local Council", "Uni Society"]

    events = []
    for eid in range(1, 881):
        beach_id = random.randint(1, 30)
        d = start_date + timedelta(days=random.randint(0, 750))
        et = choose_weighted(event_types, [0.62, 0.18, 0.20])
        sp = random.choice(sponsors)
        bags = int(max(0, round(random.gauss(22, 10))))
        events.append((eid, beach_id, d.isoformat(), et, sp, bags))

    conn.executemany("INSERT INTO cleanup_events VALUES (?,?,?,?,?,?)", events)

    # event_volunteers (composite key)
    roles = ["Leader", "Helper", "DataEntry", "Safety"]
    ev_rows = []
    for (eid, _beach_id, _d, _et, _sp, _bags) in events:
        n = random.randint(6, 14)
        ids = random.sample(range(1, 481), n)
        for j, vid in enumerate(ids):
            role = "Leader" if j == 0 else choose_weighted(roles, [0.0, 0.72, 0.16, 0.12])
            hrs = round(max(0.5, min(8.0, random.gauss(3.2, 1.2))), 1)
            ev_rows.append((eid, vid, hrs, role))

    conn.executemany("INSERT INTO event_volunteers VALUES (?,?,?,?)", ev_rows)

    # beach_daily_summary
    summary_rows = []
    for _ in range(1200):
        beach_id = random.randint(1, 30)
        d = start_date + timedelta(days=random.randint(0, 750))

        scount = int(max(0, round(random.gauss(4, 2))))
        avg = round(max(0.0, random.lognormvariate(math.log(120), 0.45)), 1)
        dom = severity_from_density(avg)

        summary_rows.append((beach_id, d.isoformat(), scount, avg, dom))

    # avoid PK clashes
    conn.executemany(
        "INSERT OR IGNORE INTO beach_daily_summary VALUES (?,?,?,?,?)",
        summary_rows,
    )

    conn.commit()

    # quick prints
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM samples;")
    print("samples:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM lab_results;")
    print("lab_results:", cur.fetchone()[0])

    cur.execute("SELECT COUNT(*) FROM beach_daily_summary;")
    print("beach_daily_summary:", cur.fetchone()[0])

    conn.close()
    print("Created:", DB_PATH)


if __name__ == "__main__":
    main()
