"""Static import checks for the Oriel Onion/Clean logical rings."""
from __future__ import annotations

import ast
from pathlib import Path
import unittest


PACKAGE = Path(__file__).resolve().parents[1] / "oriel"
RINGS = ("domain", "application", "adapters")
FORBIDDEN_INWARD_IMPORTS = {
    "argparse",
    "http",
    "importlib",
    "json",
    "os",
    "pathlib",
    "socket",
    "subprocess",
    "threading",
}


def module_name(path: Path) -> str:
    relative = path.relative_to(PACKAGE.parent).with_suffix("")
    parts = list(relative.parts)
    if parts[-1] == "__init__":
        parts.pop()
    return ".".join(parts)


def imported_modules(path: Path) -> set[str]:
    current = module_name(path)
    package = current.split(".")[:-1]
    result: set[str] = set()
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            result.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            anchor = package[: len(package) - (node.level - 1)] if node.level else []
            if node.module:
                result.add(".".join((*anchor, node.module)) if node.level else node.module)
            else:
                result.update(".".join((*anchor, alias.name)) for alias in node.names)
    return result


def dynamic_imports(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    calls: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if isinstance(node.func, ast.Name) and node.func.id in {"__import__", "import_module"}:
            calls.add(node.func.id)
        elif (
            isinstance(node.func, ast.Attribute)
            and isinstance(node.func.value, ast.Name)
            and node.func.value.id == "importlib"
            and node.func.attr == "import_module"
        ):
            calls.add("importlib.import_module")
    return calls


def ring_of(module: str) -> str | None:
    for ring in RINGS:
        if module == f"oriel.{ring}" or module.startswith(f"oriel.{ring}."):
            return ring
    if module == "oriel.__main__":
        return "composition"
    if module == "oriel":
        return "package"
    return None


class ArchitectureBoundaryTests(unittest.TestCase):
    allowed = {
        "domain": {"domain"},
        "application": {"domain", "application"},
        "adapters": {"domain", "application", "adapters"},
        "composition": {"domain", "application", "adapters", "composition", "package"},
        "package": {"application"},
    }

    def test_imports_flow_inward_by_logical_ring(self):
        for path in sorted(PACKAGE.rglob("*.py")):
            source_ring = ring_of(module_name(path))
            self.assertIsNotNone(source_ring, path)
            for imported in imported_modules(path):
                target_ring = ring_of(imported)
                if target_ring is not None:
                    self.assertIn(target_ring, self.allowed[source_ring], f"{path} imports {imported}")

    def test_application_never_imports_outer_adapters(self):
        for path in sorted((PACKAGE / "application").glob("*.py")):
            self.assertFalse(
                any(module.startswith("oriel.adapters") for module in imported_modules(path)),
                path,
            )

    def test_inward_rings_reject_dynamic_and_infrastructure_imports(self):
        for ring in ("domain", "application"):
            for path in sorted((PACKAGE / ring).glob("*.py")):
                imports = imported_modules(path)
                self.assertEqual(dynamic_imports(path), set(), path)
                self.assertFalse(
                    any(
                        module == forbidden or module.startswith(f"{forbidden}.")
                        for module in imports
                        for forbidden in FORBIDDEN_INWARD_IMPORTS
                    ),
                    path,
                )

    def test_http_adapter_only_translates_gateway_operations(self):
        imported = imported_modules(PACKAGE / "adapters" / "http.py")
        self.assertEqual(imported & {"oriel.application.text_gateway"}, {"oriel.application.text_gateway"})
        self.assertEqual(imported & {"oriel.application.ports"}, set())
        self.assertEqual(imported & {"oriel.application.startup"}, {"oriel.application.startup"})
        self.assertEqual(imported & {"oriel.domain.configuration"}, {"oriel.domain.configuration"})
        source = (PACKAGE / "adapters" / "http.py").read_text(encoding="utf-8")
        for prohibited in ("ModelPort", "ToolPort", "StatePort", "dispatch(", "record_turn(", "load_startup(", "_sessions", "_requests", "next_id("):
            self.assertNotIn(prohibited, source)
