import pytest
from ontologyops.storage import connect, ingest, build
from ontologyops.generator import write_events

@pytest.fixture
def db(tmp_path):
    events = tmp_path / 'events.jsonl'
    write_events(events, n=1200)
    with connect(tmp_path / 'test.db') as con:
        ingest(con, events)
        build(con)
        yield con
