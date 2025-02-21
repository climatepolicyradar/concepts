from contextlib import asynccontextmanager

import duckdb
from fastapi import FastAPI, HTTPException

# Global connection variable
conn = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global conn
    conn = duckdb.connect("concepts.db", read_only=True)
    yield
    # Shutdown
    if conn:
        conn.close()


app = FastAPI(title="Concepts API", lifespan=lifespan)


@app.get("/concepts/{concept_id}")
async def get_concept(concept_id: str):
    # Get column names from description
    result = conn.execute(
        """
        SELECT 
            wikibase_id,
            preferred_label,
            alternative_labels,
            negative_labels,
            description,
            definition,
            labelled_passages
        FROM concepts 
        WHERE wikibase_id = ?
    """,
        [concept_id],
    )

    columns = [desc[0] for desc in result.description]
    row = result.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Concept not found")

    concept = dict(zip(columns, row))

    related_result = conn.execute(
        """
        SELECT c.* FROM concepts c
        JOIN concept_related_relations r 
        ON c.wikibase_id = r.concept_id2 OR c.wikibase_id = r.concept_id1
        WHERE r.concept_id1 = ? OR r.concept_id2 = ?
    """,
        [concept_id, concept_id],
    )

    related_columns = [desc[0] for desc in related_result.description]
    related = [dict(zip(related_columns, r)) for r in related_result.fetchall()]

    subconcepts_result = conn.execute(
        """
        SELECT c.* FROM concepts c
        JOIN concept_subconcept_relations r 
        ON c.wikibase_id = r.subconcept_id
        WHERE r.concept_id = ?
    """,
        [concept_id],
    )

    subconcepts_columns = [desc[0] for desc in subconcepts_result.description]
    subconcepts = [
        dict(zip(subconcepts_columns, s)) for s in subconcepts_result.fetchall()
    ]

    return {"concept": concept, "related_concepts": related, "subconcepts": subconcepts}


@app.get("/search")
async def search_concepts(q: str, limit: int = 10):
    if not q:
        query = """
            SELECT
                wikibase_id,
                preferred_label,
                alternative_labels,
                negative_labels,
                description,
                definition,
                labelled_passages
            FROM concepts
            LIMIT ?
            """
    else:
        """
        SELECT
            wikibase_id,
            preferred_label,
            alternative_labels,
            negative_labels,
            description,
            definition,
            labelled_passages
        FROM concepts
        WHERE preferred_label ILIKE ?
        LIMIT ?
        """

    result = conn.execute(query, [f"{q}%", limit])

    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


@app.get("/health")
async def health_check():
    try:
        conn.execute("SELECT 1").fetchone()
        return {"status": "healthy"}
    except Exception:
        raise HTTPException(status_code=500, detail="Database connection failed")
