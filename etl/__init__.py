 (cd "$(git rev-parse --show-toplevel)" && git apply --3way <<'EOF' 
diff --git a/kpi/__init__.py b/kpi/__init__.py
new file mode 100644
index 0000000000000000000000000000000000000000..bdde5f40fa9db61db4d75cfe86f802947182f6db
--- /dev/null
+++ b/kpi/__init__.py
@@ -0,0 +1,3 @@
+"""KPI calculation package."""
+
+from .engine import KpiEngine, MetricDefinition  # noqa: F401
 
EOF
)
