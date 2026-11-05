"""Create the OpenSearch Dashboards index pattern and saved searches (as code).

    export LLM_OBS_OPENSEARCH=$(terraform -chdir=infra output -raw opensearch_endpoint)
    python scripts/setup_opensearch_dashboards.py

Run it once after the first traces have arrived (so the index exists).
Saved objects are defined in dashboards/opensearch/saved-objects.json.
"""
from __future__ import annotations

import json
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))
from _opensearch import call  # noqa: E402

HEADERS = {"osd-xsrf": "true"}
SPEC = pathlib.Path(__file__).parents[1] / "dashboards" / "opensearch" / "saved-objects.json"


def main() -> None:
    objects = json.loads(SPEC.read_text())
    for obj in objects:
        path = f"/_dashboards/api/saved_objects/{obj['type']}/{obj['id']}?overwrite=true"
        body = {"attributes": obj["attributes"]}
        if "references" in obj:
            body["references"] = obj["references"]
        call("POST", path, body, headers=HEADERS)
        print(f"created {obj['type']}: {obj['attributes'].get('title')}")
    call("POST", "/_dashboards/api/opensearch-dashboards/settings",
         {"changes": {"defaultIndex": "llm-traces"}}, headers=HEADERS)
    print("done — open Discover in OpenSearch Dashboards")


if __name__ == "__main__":
    main()
