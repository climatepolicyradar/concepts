import glob
import json

import duckdb

json_files = glob.glob("s3-concepts/concepts/*.json")

con = duckdb.connect("concepts.db")
con.execute(
    """
DROP TABLE IF EXISTS concept_related_relations;
DROP TABLE IF EXISTS concept_subconcept_relations;
DROP TABLE IF EXISTS concepts;
"""
)

con.execute(
    """
-- Main concepts table
CREATE TABLE IF NOT EXISTS concepts (
    wikibase_id VARCHAR PRIMARY KEY,
    preferred_label VARCHAR,
    alternative_labels VARCHAR[],
    negative_labels VARCHAR[],
    description VARCHAR,
    definition VARCHAR,
    labelled_passages JSON
);

-- Relationship tables with unique constraints
CREATE TABLE IF NOT EXISTS concept_subconcept_relations (
    concept_id VARCHAR,
    subconcept_id VARCHAR,
    FOREIGN KEY (concept_id) REFERENCES concepts(wikibase_id),
    FOREIGN KEY (subconcept_id) REFERENCES concepts(wikibase_id),
    UNIQUE(concept_id, subconcept_id)  -- Prevents duplicate relationships
);

CREATE TABLE IF NOT EXISTS concept_related_relations (
    concept_id1 VARCHAR,
    concept_id2 VARCHAR,
    FOREIGN KEY (concept_id1) REFERENCES concepts(wikibase_id),
    FOREIGN KEY (concept_id2) REFERENCES concepts(wikibase_id),
    UNIQUE(concept_id1, concept_id2)  -- Prevents duplicate relationships
);
"""
)

# First pass: Insert all concepts
for file_path in json_files:
    with open(file_path, "r") as f:
        data = json.load(f)

    # Insert main concept data with ON CONFLICT DO NOTHING for deduplication
    con.execute(
        """
        INSERT INTO concepts VALUES (?, ?, ?, ?, ?, ?, ?)
    """,
        (
            data["wikibase_id"],
            data["preferred_label"],
            data["alternative_labels"],
            data["negative_labels"],
            data["description"],
            data["definition"],
            data["labelled_passages"],
        ),
    )

# Second pass: Insert all relationships
missing_concepts = set()

for file_path in json_files:
    with open(file_path, "r") as f:
        data = json.load(f)

    # Insert subconcept relationships
    for subconcept_id in data["subconcept_of"]:
        try:
            con.execute(
                """
                INSERT INTO concept_subconcept_relations (concept_id, subconcept_id)
                VALUES (?, ?)
            """,
                (data["wikibase_id"], subconcept_id),
            )
        except duckdb.ConstraintException as e:
            print(f"Error: {e}")
            missing_concepts.add(subconcept_id)

    # Insert related concept relationships
    for related_id in data["related_concepts"]:
        # Only insert if concept_id1 < concept_id2 to avoid duplicates
        if data["wikibase_id"] < related_id:
            try:

                con.execute(
                    """
                    INSERT INTO concept_related_relations (concept_id1, concept_id2)
                    VALUES (?, ?)
                """,
                    (data["wikibase_id"], related_id),
                )
            except duckdb.ConstraintException as e:
                print(f"Error: {e}")
                missing_concepts.add(related_id)

print(f"Done. Found {len(missing_concepts)} missing concept IDs:")
for concept_id in sorted(missing_concepts):
    print(f"  - {concept_id}")

print("Done")
