"""
Coastal Microplastic Monitoring & Cleanup Database (SQLite)

This script generates a synthetic SQLite database from scratch (no external datasets).
It is designed for coursework requirements:
- Multiple related tables with foreign keys
- At least one table with 1000+ rows (samples)
- Nominal / ordinal / interval / ratio data types
- Deliberate small amounts of missing and duplicate data for realism

Run:
  python3 database_commented.py

Output:
  coastal_microplastics_v3.sqlite
"""


import sqlite3, random, math, os
from datetime import date, timedelta
from collections import defaultdict, Counter

DB_PATH = "coastal_microplastics_v3.sqlite"

# DB_PATH controls the output file name. Using a single file keeps submission simple.

SCHEMA_SQL = r'''# The schema is embedded so the DB can be rebuilt anywhere without extra files.
# CHECK constraints are used to prevent impossible values and improve realism.
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS beaches (
    beach_id     INTEGER PRIMARY KEY,
    beach_name   TEXT NOT NULL,
    region       TEXT NOT NULL,
    latitude     REAL NOT NULL CHECK (latitude BETWEEN 49.0 AND 61.5),
    longitude    REAL NOT NULL CHECK (longitude BETWEEN -8.5 AND 2.5),
    risk_level   TEXT NOT NULL CHECK (risk_level IN ('Low','Medium','High','Critical'))
);

CREATE TABLE IF NOT EXISTS volunteers (
    volunteer_id      INTEGER PRIMARY KEY,
    signup_date       TEXT NOT NULL,
    age_years         INTEGER NOT NULL CHECK (age_years BETWEEN 16 AND 90),
    experience_level  TEXT NOT NULL CHECK (experience_level IN ('Beginner','Intermediate','Advanced')),
    home_region       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cleanup_events (
    event_id          INTEGER PRIMARY KEY,
    beach_id          INTEGER NOT NULL,
    event_date        TEXT NOT NULL,
    start_hour        INTEGER NOT NULL CHECK (start_hour BETWEEN 0 AND 23),
    duration_minutes  INTEGER NOT NULL CHECK (duration_minutes BETWEEN 15 AND 360),
    temperature_c     REAL NOT NULL,
    weather_type      TEXT,
    FOREIGN KEY (beach_id) REFERENCES beaches(beach_id)
        ON UPDATE CASCADE ON DELETE RESTRICT,
    CHECK (weather_type IS NULL OR weather_type IN ('Sunny','Cloudy','Windy','Rain','Storm'))
);

CREATE TABLE IF NOT EXISTS event_volunteers (
    event_id      INTEGER NOT NULL,
    volunteer_id  INTEGER NOT NULL,
    role          TEXT NOT NULL CHECK (role IN ('Collector','Sorter','DataEntry','Supervisor')),
    hours_worked  REAL NOT NULL CHECK (hours_worked > 0 AND hours_worked <= 8),
    PRIMARY KEY (event_id, volunteer_id),
    FOREIGN KEY (event_id) REFERENCES cleanup_events(event_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    FOREIGN KEY (volunteer_id) REFERENCES volunteers(volunteer_id)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS samples (
    sample_id              INTEGER PRIMARY KEY,
    beach_id               INTEGER NOT NULL,
    collection_date        TEXT NOT NULL,
    collection_method      TEXT NOT NULL CHECK (collection_method IN ('Transect','Quadrat','Grab','Sieve')),
    dominant_polymer_type  TEXT,
    plastic_density_mg_m2  REAL NOT NULL CHECK (plastic_density_mg_m2 >= 0),
    pollution_severity     TEXT NOT NULL CHECK (pollution_severity IN ('Low','Moderate','High','Severe')),
    FOREIGN KEY (beach_id) REFERENCES beaches(beach_id)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE TABLE IF NOT EXISTS lab_results (
    result_id             INTEGER PRIMARY KEY,
    sample_id             INTEGER NOT NULL UNIQUE,
    microplastic_count    INTEGER NOT NULL CHECK (microplastic_count >= 0),
    avg_particle_size_mm  REAL NOT NULL CHECK (avg_particle_size_mm >= 0),
    analysis_method       TEXT NOT NULL CHECK (analysis_method IN ('FTIR','Raman','Microscopy','Py-GCMS')),
    analyst_confidence    TEXT,
    FOREIGN KEY (sample_id) REFERENCES samples(sample_id)
        ON UPDATE CASCADE ON DELETE CASCADE,
    CHECK (analyst_confidence IS NULL OR analyst_confidence IN ('Low','Medium','High'))
);

CREATE TABLE IF NOT EXISTS beach_daily_summary (
    beach_id          INTEGER NOT NULL,
    summary_date      TEXT NOT NULL,
    sample_count      INTEGER NOT NULL CHECK (sample_count >= 0),
    avg_density_mg_m2 REAL NOT NULL CHECK (avg_density_mg_m2 >= 0),
    dominant_severity TEXT NOT NULL CHECK (dominant_severity IN ('Low','Moderate','High','Severe')),
    PRIMARY KEY (beach_id, summary_date),
    FOREIGN KEY (beach_id) REFERENCES beaches(beach_id)
        ON UPDATE CASCADE ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS idx_samples_beach_date ON samples(beach_id, collection_date);
CREATE INDEX IF NOT EXISTS idx_events_beach_date ON cleanup_events(beach_id, event_date);
CREATE INDEX IF NOT EXISTS idx_lab_results_method ON lab_results(analysis_method);
'''

def choose_weighted(options, weights):
    # Weighted selection avoids overly-uniform categorical data.
    r = random.random() * sum(weights)
    upto = 0.0
    for o, w in zip(options, weights):
        upto += w
        if upto >= r:
            return o
    return options[-1]

def seasonal_temp_c(d):
    # Seasonal model (interval scale): temperature varies through the year.
    doy = d.timetuple().tm_yday
    mean = 11 + 7 * math.sin(2 * math.pi * (doy - 205) / 365.0)
    return round(mean + random.gauss(0, 2.0), 1)

def severity_from_density(x):
    # Ordinal label derived from ratio-scale density.
    if x < 80: return "Low"
    if x < 180: return "Moderate"
    if x < 320: return "High"
    return "Severe"

def main():
    # Seed makes results reproducible for marking.
    random.seed(42)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    # Delete old DB so the script always builds from scratch.
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(SCHEMA_SQL)

    # Table sizes are chosen to look realistic:
    # - beaches is small (reference table)
    # - volunteers and events are medium
    # - samples and lab_results are large measurement tables

    regions=["North","NorthWest","NorthEast","Midlands","East","SouthEast","SouthWest","South"]
    risk_levels=["Low","Medium","High","Critical"]
    polymer_types=["PET","PE","PP","PS","PVC","Nylon","PU","Other"]
    collection_methods=["Transect","Quadrat","Grab","Sieve"]
    weather_types=["Sunny","Cloudy","Windy","Rain","Storm"]
    roles=["Collector","Sorter","DataEntry","Supervisor"]
    experience_levels=["Beginner","Intermediate","Advanced"]
    analysis_methods=["FTIR","Raman","Microscopy","Py-GCMS"]
    confidence_levels=["Low","Medium","High"]

    beach_names=[]
    # Deliberate duplicates: add a repeated beach name to mimic naming overlap.
    for _ in range(28):
        beach_names.append(f"{random.choice(['Sandy','Pebble','Harbour','Cliff','Dune','Seabrook','Brightwater','Driftwood','Silver','Blue'])} {random.choice(['Bay','Shore','Cove','Beach','Point','Sands'])}")
    beach_names += ["Seabrook Bay","Seabrook Bay"]
    random.shuffle(beach_names)

    beaches=[]
    for i in range(1,31):
        beaches.append((i,beach_names[i-1],random.choice(regions),round(49.0 + random.random()*12.5,5),round(-8.5 + random.random()*11.0,5),choose_weighted(risk_levels,[0.35,0.35,0.2,0.1])))
    conn.executemany("INSERT INTO beaches VALUES (?,?,?,?,?,?)", beaches)

    volunteers=[]
    start_signup=date(2023,1,1)
    for vid in range(1,451):
        signup=start_signup + timedelta(days=random.randint(0,1100))
        age=int(min(90,max(16,round(random.gauss(33,12)))))
        volunteers.append((vid,signup.isoformat(),age,choose_weighted(experience_levels,[0.55,0.33,0.12]),random.choice(regions)))
    conn.executemany("INSERT INTO volunteers VALUES (?,?,?,?,?)", volunteers)

    event_start=date(2024,1,1)
    event_end=date(2026,2,15)
    num_days=(event_end-event_start).days+1

    events=[]
    for eid in range(1,851):
        d=event_start + timedelta(days=random.randint(0,num_days-1))
        weather=choose_weighted(weather_types,[0.32,0.30,0.18,0.16,0.04])
        if random.random() < 0.03:
            weather=None
        # Deliberate missingness: weather sometimes unrecorded.
        events.append((eid,random.randint(1,30),d.isoformat(),choose_weighted(list(range(6,19)),[2,3,4,5,6,7,7,6,5,4,3,2,2]),int(max(15,min(360,round(random.gauss(140,55))))),float(seasonal_temp_c(d)),weather))
    conn.executemany("INSERT INTO cleanup_events VALUES (?,?,?,?,?,?,?)", events)

    ev=[]
    for (eid,beach_id,d,sh,dur,temp,wea) in events:
        n=int(max(3,min(30,round(random.gauss(11,4)))))
        vids=random.sample(range(1,451), k=min(n,450))
        base_hours=min(6.0,max(1.0,dur/60.0 + random.gauss(0,0.3)))
        for v in vids:
            ev.append((eid,v,choose_weighted(roles,[0.55,0.25,0.12,0.08]),round(min(8.0,max(0.5,base_hours + random.gauss(0,0.4))),1)))
    conn.executemany("INSERT INTO event_volunteers VALUES (?,?,?,?)", ev)

    samples=[]
    for sid in range(1,5001):
        beach_id=random.randint(1,30)
        d=event_start + timedelta(days=random.randint(0,num_days-1))
        polymer=choose_weighted(polymer_types,[0.22,0.20,0.18,0.10,0.05,0.08,0.07,0.10])
        if random.random() < 0.04:
            polymer=None
        # Deliberate missingness: polymer type sometimes unknown.
        beach_risk=beaches[beach_id-1][5]
        risk_mult={"Low":0.85,"Medium":1.0,"High":1.25,"Critical":1.6}[beach_risk]
        density=max(0.0, random.lognormvariate(math.log(140),0.55)*risk_mult)
        # Log-normal distribution produces realistic right-skewed pollution.
        density=round(density,1)
        samples.append((sid,beach_id,d.isoformat(),choose_weighted(collection_methods,[0.35,0.30,0.20,0.15]),polymer,float(density),severity_from_density(density)))
    conn.executemany("INSERT INTO samples VALUES (?,?,?,?,?,?,?)", samples)

    lab=[]
    for rid,(sid,beach_id,cd,method,polymer,density,sev) in enumerate(samples, start=1):
        count=int(max(0, round((density*random.uniform(0.9,1.3))*random.uniform(0.8,1.2))))
        size=max(0.05, random.gauss(1.4,0.45))
        if sev=="Severe": size*=0.75
        elif sev=="High": size*=0.9
        size=round(size,2)
        conf=choose_weighted(confidence_levels,[0.12,0.45,0.43])
        if random.random() < 0.03:
            conf=None
        # Deliberate missingness: analyst confidence not always recorded.
        lab.append((rid,sid,count,float(size),choose_weighted(analysis_methods,[0.35,0.20,0.30,0.15]),conf))
    conn.executemany("INSERT INTO lab_results VALUES (?,?,?,?,?,?)", lab)

    grp=defaultdict(list)
    for (sid,beach_id,cd,method,polymer,density,sev) in samples:
        grp[(beach_id,cd)].append((density,sev))
    summary=[]
    # beach_daily_summary has a composite key (beach_id, summary_date) for daily aggregates.
    for beach_id in range(1,31):
        keys=[k for k in grp.keys() if k[0]==beach_id]
        random.shuffle(keys)
        for (b,cd) in keys[:40]:
            vals=grp[(b,cd)]
            sc=len(vals)
            avg=round(sum(v[0] for v in vals)/sc,1)
            dom=Counter(v[1] for v in vals).most_common(1)[0][0]
            summary.append((b,cd,sc,float(avg),dom))
    conn.executemany("INSERT INTO beach_daily_summary VALUES (?,?,?,?,?)", summary)

    conn.commit()
    conn.close()

if __name__ == "__main__":
    main()
