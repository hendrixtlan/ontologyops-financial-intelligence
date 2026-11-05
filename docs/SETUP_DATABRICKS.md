# Bring V0.1 into Azure Databricks

**Prerequisites:** Azure Databricks workspace with Unity Catalog, a SQL warehouse, and compute that supports
Metric View YAML specification 1.1. Workspace feature access/entitlements must allow Genie One, Unity Gateway,
and (if desired) Genie Ontology Public Preview and Unity Catalog Pages.

1. Locally run `ontologyops seed` and locate `data/generated/raw_payment_events.jsonl`.
2. In the workspace, create a schema `main.ontologyops` (or adjust all qualified assets consistently),
   create a UC Volume in `main.ontologyops` named `landing`, and upload the synthetic JSONL there.
3. Authenticate Databricks CLI. Run `databricks bundle validate -t dev`, then
   `databricks bundle deploy -t dev` and `databricks bundle run ontologyops_reference_snapshot -t dev`.
   If the job definition needs an explicit compute setting in your workspace, select serverless job compute or
   configure an approved compute policy for the notebook task.
4. Generate metric DDL with `ontologyops render-sql > metric_view.sql`; execute in Databricks SQL editor.
   Then execute `sql/01_metric_queries.sql` and compare against `ontologyops metrics` locally.
5. Curate or import `semantics/pages/settlement_business_glossary.md` as a governed Unity Catalog Page;
   this is a manual workspace publishing step, not a simulated Page API call.
6. Discover the server in Unity Gateway: `system.ai.genie_one_mcp`. Grant the calling identity `EXECUTE`
   and appropriate SELECT/USE permissions. On-behalf-of-user OAuth is preferred; the example CLI accepts a
   scoped `DATABRICKS_TOKEN` for a developer's proof of concept. Avoid a long-lived PAT in production.
7. The MCP URL is
   `https://<workspace-host>/ai-gateway/mcp-services/system.ai.genie_one_mcp?o=<workspace-id>`.
   For example, with a token in the environment:

   ```bash
   pip install -e '.[genie]'
   ontologyops genie-ask --host https://your-workspace.azuredatabricks.net \
     --workspace-id 123456789 --question 'What is our MX-US settlement failure rate by acquirer on 2026-11-03?'
   ```

   `genie_ask` initiates an asynchronous response; use `genie_poll_response` with returned IDs to retrieve
   completion. This release intentionally exposes the raw response envelope rather than inventing a completed
   answer or silently polling with undocumented tool schema assumptions.

## Security

- Never commit `.env` or tokens; set short-lived environment credentials using a secure secrets service.
- Run as a principal with least-privilege UC grants; access is determined by Unity Gateway and UC.
- The raw transactions are synthetic and contain no real personal payment data.
- Review candidate metric/view/glossary changes via pull requests and run the semantic regression gate.
