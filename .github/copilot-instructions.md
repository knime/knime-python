# KNIME Python Integration

Hybrid Java/Python/JavaScript codebase bridging KNIME Analytics Platform with Python via Py4J and Apache Arrow.

## Architecture Overview

**Components**:
- `org.knime.python3*`: OSGi plugins (Eclipse bundles with `META-INF/MANIFEST.MF`)
- `org.knime.python3.scripting.nodes/js-src/`: Vue 3 scripting editor
- `knime-extension/`: Conda package for Python node development

**Java-Python Bridge**: Py4J enables bidirectional communication. Java spawns Python processes via `PythonGateway`; Python extends `EntryPoint` classes to expose functionality.

**Data Transfer**: Apache Arrow IPC format via memory-mapped files. Tables serialized through `PythonArrowDataSource`/`PythonArrowDataSink`, metadata (types/domain) passed as JSON.

**Node Extension Flow**:
1. Python defines nodes with `@knext.node`/`@knext.parameter` decorators
2. Java's `PurePythonNodeSetFactory` discovers them via Py4J at KNIME startup
3. `DelegatingNodeModel` proxies execution to Python via `NodeProxy` interfaces
4. Data flows through Arrow, callbacks through Py4J (e.g., `LogCallback`, `AuthCallback`)

## Module Structure

```
org.knime.python3           # Core Py4J gateway
org.knime.python3.arrow     # Arrow data transfer (Java)
org.knime.python3.arrow.types  # KNIME types (Python)
org.knime.python3.nodes     # Node framework (Java)
org.knime.python3.scripting # Scripting infrastructure
org.knime.python3.scripting.nodes  # Script nodes + Vue editor
org.knime.python3.views     # Python view rendering
org.knime.ext.py4j          # Py4J OSGi wrapper
*.py4j.dependencies         # Expose classes to py4j classloader (don't merge!)
```

## Quick Start

```bash
pixi install                # Setup Python env (.pixi/envs/default)
pixi run test-all           # Python tests (interactive env)
mvn clean verify            # Java tests
cd org.knime.python3.scripting.nodes/js-src && npm ci && npm run coverage  # JS tests
```

## CI/CD

- **Jenkinsfile**: Primary CI
- **GitHub Actions**: Fast pre-checks: lint, test, build JS, format Python
