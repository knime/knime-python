---
applyTo: "**/*.java"
---
# Java Development - KNIME Python Integration

## Build System (Tycho, NOT Standard Maven)
```bash
mvn clean verify    # Full build + tests (Tycho 4.0.13 in .mvn/extensions.xml)
```

**P2 Dependencies** (root `pom.xml`): Dependencies from Eclipse P2 repos, NOT Maven Central.

## Repository-Specific Conventions
- **OSGi bundles**: Each `org.knime.*` folder has `META-INF/MANIFEST.MF` + `build.properties`
- **Test bundles**: Name ends `.tests`, use `eclipse-test-plugin` packaging
- **Test POMs**: Require `org.knime.features.clfixes`, Arrow tests need `org.knime.features.core.columnar`
- **Critical**: `*.py4j.dependencies` bundles expose classes to py4j classloader - **don't merge!**

## Key Classes (Repository-Specific)
- `PythonGateway`: Spawns Python processes
- `PythonArrowDataSource`/`Sink`: Arrow I/O with Python (temp files, metadata as JSON)
- `DelegatingNodeModel`: Proxies node execution to Python
- `PurePythonNodeSetFactory`: Discovers Python nodes via Py4J at startup
- `QueuedPythonGatewayFactory`: Gateway lifecycle singleton

## Common Pitfalls
1. **P2 only**: Can't use Maven Central deps
2. **OSGi classloading**: Keep `*.py4j.dependencies` separate
3. **Test failures**: Missing `org.knime.features.clfixes` in test POM
4. **Bundle exports**: Explicitly list in `MANIFEST.MF`
