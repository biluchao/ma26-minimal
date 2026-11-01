"""core/module_manifest.py — Module manifest + dependency resolver"""

import re
from dataclasses import dataclass, field


@dataclass
class DependencySpec:
    name: str
    version_range: str = "*"
    optional: bool = False

    def matches(self, version: str) -> bool:
        return self._match(version, self.version_range)

    @staticmethod
    def _match(version, spec):
        if spec.strip() == "*":
            return True
        for cond in [c.strip() for c in spec.split(',')]:
            if not DependencySpec._single(version, cond):
                return False
        return True

    @staticmethod
    def _single(version, cond):
        m = re.match(r'^(>=|<=|==|!=|>|<|~=)?\s*(.+)$', cond)
        if not m:
            return False
        op, target = m.group(1) or "==", m.group(2).strip()
        if target == "*":
            return True
        va = DependencySpec._parse(version)
        vb = DependencySpec._parse(target)
        if not va or not vb:
            return False
        c = DependencySpec._cmp(va, vb)
        return {'>=': c >= 0, '<=': c <= 0, '==': c == 0, '!=': c != 0,
                '>': c > 0, '<': c < 0, '~=': va[0] == vb[0] and c >= 0}.get(op, False)

    @staticmethod
    def _parse(v):
        m = re.match(r'^v?(\d+)\.(\d+)\.(\d+).*$', v)
        return tuple(int(x) for x in m.groups()) if m else None

    @staticmethod
    def _cmp(a, b):
        for x, y in zip(a, b):
            if x < y:
                return -1
            if x > y:
                return 1
        return 0


@dataclass
class InterfaceSpec:
    name: str
    version: str = "1.0.0"

    def compatible_with(self, other: 'InterfaceSpec') -> bool:
        return self.name == other.name and self.version.split('.')[0] == other.version.split('.')[0]


@dataclass
class ModuleManifest:
    name: str
    version: str
    description: str = ""
    interfaces_provided: list = field(default_factory=list)
    interfaces_required: list = field(default_factory=list)
    dependencies: list = field(default_factory=list)
    config_schema: dict = field(default_factory=dict)
    resource_quota: dict = field(default_factory=dict)
    tags: list = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> 'ModuleManifest':
        return cls(
            name=d['name'], version=d['version'],
            description=d.get('description', ''),
            interfaces_provided=[InterfaceSpec(**i) for i in d.get('provides', [])],
            interfaces_required=[InterfaceSpec(**i) for i in d.get('requires', [])],
            dependencies=[DependencySpec(**dep) for dep in d.get('dependencies', [])],
            config_schema=d.get('config_schema', {}),
            resource_quota=d.get('resource_quota', {}),
            tags=d.get('tags', []),
        )


class DependencyResolver:
    """Resolves module dependencies, checks compatibility and cycles."""

    def __init__(self, manifests: list):
        self.m = {mf.name: mf for mf in manifests}
        self.errors = []

    def resolve(self) -> tuple:
        self.errors.clear()
        self._check_requires()
        self._check_versions()
        self._check_cycles()
        self._check_interface_compat()
        return len(self.errors) == 0, self.errors

    def _check_requires(self):
        provided = {}
        for n, mf in self.m.items():
            for iface in mf.interfaces_provided:
                provided[iface.name] = n
        for n, mf in self.m.items():
            for req in mf.interfaces_required:
                if req.name not in provided:
                    self.errors.append(
                        f"'{n}' requires '{req.name}' v{req.version} but no module provides it")

    def _check_versions(self):
        for n, mf in self.m.items():
            for dep in mf.dependencies:
                if dep.name not in self.m:
                    if not dep.optional:
                        self.errors.append(f"'{n}' requires '{dep.name}' {dep.version_range} not registered")
                    continue
                if not dep.matches(self.m[dep.name].version):
                    self.errors.append(
                        f"'{n}' requires '{dep.name}' {dep.version_range} but found v{self.m[dep.name].version}")

    def _check_cycles(self):
        visited, stack = set(), set()

        def dfs(node, path):
            visited.add(node)
            stack.add(node)
            mf = self.m.get(node)
            if mf:
                for dep in mf.dependencies:
                    if dep.name in stack:
                        cycle = path[path.index(dep.name):] + [dep.name]
                        self.errors.append("Circular dependency: " + " → ".join(cycle))
                        return True
                    if dep.name not in visited and dep.name in self.m:
                        if dfs(dep.name, path + [dep.name]):
                            return True
            stack.discard(node)
            return False

        for n in self.m:
            if n not in visited:
                dfs(n, [n])

    def _check_interface_compat(self):
        providers = {}
        for n, mf in self.m.items():
            for iface in mf.interfaces_provided:
                providers.setdefault(iface.name, []).append((n, iface))
        for n, mf in self.m.items():
            for req in mf.interfaces_required:
                for pname, iface in providers.get(req.name, []):
                    if not req.compatible_with(iface):
                        self.errors.append(
                            f"'{req.name}' v{req.version} required by '{n}' but '{pname}' provides v{iface.version}")

    def topological_sort(self) -> list:
        """Kahn's algorithm for startup order."""
        indeg = {n: 0 for n in self.m}
        graph = {n: [] for n in self.m}
        for n, mf in self.m.items():
            for dep in mf.dependencies:
                if dep.name in self.m:
                    graph[dep.name].append(n)
                    indeg[n] += 1
        from collections import deque
        q = deque([n for n, d in indeg.items() if d == 0])
        out = []
        while q:
            node = q.popleft()
            out.append(node)
            for nb in graph[node]:
                indeg[nb] -= 1
                if indeg[nb] == 0:
                    q.append(nb)
        return out

