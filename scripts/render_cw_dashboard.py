"""Render dashboards/cloudwatch-overview.json.tftpl the way Terraform does and
validate it — used in CI, and handy for `aws cloudwatch put-dashboard` by hand.

    python scripts/render_cw_dashboard.py dev > /tmp/dash.json
"""
from __future__ import annotations

import json
import pathlib
import re
import sys

env = sys.argv[1] if len(sys.argv) > 1 else "dev"
tpl = (pathlib.Path(__file__).parents[1] / "dashboards" / "cloudwatch-overview.json.tftpl").read_text()
values = {
    "namespace": f"LLMObservability/{env}",
    "region": "us-east-1",
    "processor_function": f"llm-obs-{env}-processor",
    "canary_function": f"llm-obs-{env}-canary",
    "alarm_arns": json.dumps([f"arn:aws:cloudwatch:us-east-1:123456789012:alarm:llm-obs-{env}-canary-failing"]),
}
rendered = re.sub(r"\$\{(\w+)\}", lambda m: values[m.group(1)], tpl)
dash = json.loads(rendered)
assert all({"type", "x", "y", "width", "height", "properties"} <= w.keys() for w in dash["widgets"])
print(json.dumps(dash, indent=2))
