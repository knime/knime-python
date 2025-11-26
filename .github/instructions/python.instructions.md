---
applyTo: "**/*.py"
---
# Python Development - KNIME Python Integration

## Dependencies & Testing
```bash
pixi install          # Setup .pixi/envs/default
pixi run test-all     # Test across py38-py314, KNIME 4.7-5.6
pixi run format       # Ruff (target: Python 3.8)
```

**All dependencies via `pixi.toml`** - never pip/conda directly.

## Repository-Specific Conventions
- **Path setup**: Root `conftest.py` adds all plugin paths to `sys.path` - **never modify `sys.path` in tests**
- **Test style**: Function-based tests only (`pytest.ini` disables class discovery)
- **Multi-version**: Changes must work across Python 3.8-3.14 - test with `pixi run test-all`
- **PyArrow versions**: Tightly coupled to Python version in `pixi.toml` - don't update independently

## Key Modules (Repository-Specific)
- `knext`: Node extension decorators (`@knext.node`, `@knext.parameter`)
- `knime.scripting`: Script API (replaces deprecated `knime_io`, `knime_jupyter`)
- `knime._arrow._backend`: Arrow I/O with Java (via Py4J)

## Py4J Integration
- Extend Java `EntryPoint` classes to expose Python to Java
- Implement Java interfaces for callbacks (`LogCallback`, `AuthCallback`)
- Arrow data via temp files (IPC format), metadata as JSON

## Debugging
- `KNIME_PYTHON_DEBUG=true` for gateway logs
- Import failures: check `conftest.py`
