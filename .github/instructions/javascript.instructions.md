---
applyTo: "org.knime.python3.scripting.nodes/js-src/**"
---
# JavaScript/TypeScript Development - KNIME Python Integration

## Location
`org.knime.python3.scripting.nodes/js-src/` - Python scripting editor dialog

## Setup
```bash
cd org.knime.python3.scripting.nodes/js-src
npm ci              # Node 22.11.0, npm 10.9.1
npm run coverage    # Tests + coverage (do not use `npm run test:unit` - it's interactive)
```

## Repository-Specific Conventions
- **Maven integration**: `frontend-maven-plugin` runs npm during Maven build
- **Dependency**: Uses `@knime/scripting-editor` - unlink before committing if testing local changes

## KNIME Dev Mode
Start KNIME with:
```
-Dchromium.remote_debugging_port=8988
-Dorg.knime.ui.dev.node.dialog.url=http://localhost:5173/
```
Then: `npm run dev:knime`
