from pathlib import Path
import yaml
from ontologyops.paths import DEFAULT_METRIC_VIEW, ROOT
from ontologyops.semantics import detect_drift, render_metric_ddl
from ontologyops.evaluation import validate_metric


def test_clean_contract_and_kpi(db):
    drift = detect_drift(DEFAULT_METRIC_VIEW)
    regression = validate_metric(db)
    assert drift['status'] == 'pass'
    assert regression['status'] == 'pass'
    assert regression['groups_checked'] > 0


def test_bad_contract_blocked(db):
    bad = ROOT / 'examples' / 'invalid_payment_health.yaml'
    result = detect_drift(bad)
    assert result['status'] == 'blocked'
    assert any(f['code'] == 'CERTIFIED_FORMULA_DRIFT' for f in result['findings'])
    execution = validate_metric(db, bad)
    assert execution['status'] == 'fail'
    assert len(execution['mismatches']) > 0


def test_harmless_whitespace_passes(tmp_path):
    contents = DEFAULT_METRIC_VIEW.read_text(encoding='utf-8')
    contents = contents.replace("CAST(SUM(", "CAST(  SUM(")
    candidate = tmp_path / 'whitespace.yaml'
    candidate.write_text(contents, encoding='utf-8')
    assert detect_drift(candidate)['status'] == 'pass'


def test_data_source_tampering_blocks(tmp_path):
    doc = yaml.safe_load(DEFAULT_METRIC_VIEW.read_text(encoding='utf-8'))
    doc['source'] = 'main.shadow.gold_payment_outcomes'
    candidate = tmp_path / 'changed-source.yaml'
    candidate.write_text(yaml.safe_dump(doc), encoding='utf-8')
    result = detect_drift(candidate)
    assert 'SOURCE_CHANGE' in {r['code'] for r in result['findings']}


def test_dimension_rebinding_blocks(tmp_path):
    doc = yaml.safe_load(DEFAULT_METRIC_VIEW.read_text(encoding='utf-8'))
    next(field for field in doc['fields'] if field['name'] == 'currency')['expr'] = "'USD'"
    candidate = tmp_path / 'rebound-currency.yaml'
    candidate.write_text(yaml.safe_dump(doc), encoding='utf-8')
    result = detect_drift(candidate)
    assert 'DIMENSION_FORMULA_DRIFT' in {r['code'] for r in result['findings']}


def test_empty_denominator_is_null(db):
    db.execute("INSERT INTO gold_payment_outcomes VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        ('isolated-pending', '2026-12-01', 'm-demo', 'enterprise', 'ACQ_C', 'MX-US', 'USD', 1234, 'PENDING'))
    result = validate_metric(db)
    assert result['status'] == 'pass'
    assert result['groups_checked'] == 9


def test_generated_databricks_ddl_is_current_syntax():
    ddl = render_metric_ddl()
    assert 'WITH METRICS LANGUAGE YAML AS' in ddl
    assert 'version: ' in ddl
    assert 'payment_health' in ddl
