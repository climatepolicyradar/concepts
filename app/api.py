import logging
from contextlib import asynccontextmanager
from dataclasses import Field
from typing import List, Optional
import socket
import duckdb
from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.trace import Status, StatusCode
hostname = socket.gethostname()

resource = Resource(
    attributes={
        "service.name": os.getenv("OTEL_SERVICE_NAME"),
        "host.name": hostname,
        "resource.attributes": os.getenv("OTEL_RESOURCE_ATTRIBUTES"),
        "service.instance.id": "henry-local"
    }
)
tracer_provider = TracerProvider(resource=resource)

trace.set_tracer_provider(tracer_provider)
otlp_exporter = OTLPSpanExporter(
    endpoint=f"{os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT")}/v1/traces",
    headers={"Authorization": f"Basic {os.getenv('OTEL_EXPORTER_OTLP_TOKEN')}"},
)
tracer_provider.add_span_processor(BatchSpanProcessor(otlp_exporter))

tracer = trace.get_tracer(__name__)

# Global connection variable
conn = None

_LOGGER = logging.getLogger(__name__)
_LOGGER.setLevel(logging.DEBUG)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    global conn
    conn = duckdb.connect("concepts.db", read_only=True)
    yield
    # Shutdown
    if conn:
        conn.close()


router = APIRouter(
    prefix="/concepts",
)
app = FastAPI(
    title="Concepts API",
    lifespan=lifespan,
    docs_url="/concepts/docs",  # Docs will be available at /router_prefix/docs after mounting
    redoc_url="/concepts/redoc",
    openapi_url="/concepts/openapi.json",
)

@router.get("/search")
@tracer.start_as_current_span("search_concepts")
async def search_concepts(q: Optional[str] = None, limit: int = 10):
    span = trace.get_current_span()
    span.add_event(f"Beginning search for {q}")

    if q and q.startswith("FORCE_ERROR"):
        try:
            result = conn.execute("BLEEP BLOOP")
        except Exception as e:
            span.set_status(Status(StatusCode.ERROR))
            span.record_exception(e)
            raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")
        
    if not q:
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
            LIMIT ?
            """,
            [limit],
        )
    else:
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
            WHERE preferred_label ILIKE ?
            LIMIT ?
            """,
            [f"{q}%", limit],
    )
    all_results = result.fetchall()
    
    span.add_event(f"Found {len(all_results)} results")
    span.set_attribute("query", q)
    span.set_attribute("limit", limit)
    span.set_attribute("result_count", len(all_results))
    span.set_status(Status(StatusCode.OK))

    columns = [desc[0] for desc in result.description]
    span.add_event(f"Returning {len(columns)} columns")
    return [dict(zip(columns, row)) for row in all_results]


class BatchSearchModel(BaseModel):
    ids: List[str] = Field(Query(default=[]))


@router.get("/batch_search")
async def batch_search_concepts(dto: BatchSearchModel = Depends()):
    """Search for multiple concepts by their wikibase IDs.

    :param ids: List of wikibase IDs to search for
    :type ids: BatchSearchModel
    :raises HTTPException: If no IDs provided or database error
    :return: List of found concepts (may be empty if no matches)
    :rtype: List[dict]
    """
    print(f"🔍 Searching for {len(dto.ids)} concepts")
    if not dto.ids:
        raise HTTPException(status_code=400, detail="No IDs provided")

    print(dto.ids)
    try:
        # Use string concatenation to create the IN clause
        placeholders = ",".join([f"'{id}'" for id in dto.ids])
        query = f"""
            SELECT
                wikibase_id,
                preferred_label,
                alternative_labels,
                negative_labels,
                description,
                definition,
                labelled_passages
            FROM concepts
            WHERE wikibase_id IN ({placeholders})
        """
        result = conn.execute(query)
        columns = [desc[0] for desc in result.description]
        matches = [dict(zip(columns, row)) for row in result.fetchall()]

        # Log missing IDs for debugging
        found_ids = {match["wikibase_id"] for match in matches}
        missing_ids = set(dto.ids) - found_ids

        if missing_ids:
            _LOGGER.warning(f"🕵️ Missing IDs: {missing_ids}")

        return matches or []
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")


@router.get("/{concept_id}")
@tracer.start_as_current_span("get_concept")
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


@router.get("/health")
async def health_check():
    try:
        conn.execute("SELECT 1").fetchone()
        return {"status": "healthy"}
    except Exception:
        raise HTTPException(status_code=500, detail="Database connection failed")


app.include_router(router)
