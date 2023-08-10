# Python Scripting Editor

The scripting editor dialog for the Python scripting nodes.

## Recommended IDE Setup

[VSCode](https://code.visualstudio.com/) + [Volar](https://marketplace.visualstudio.com/items?itemName=Vue.volar) (and disable Vetur) + [TypeScript Vue Plugin (Volar)](https://marketplace.visualstudio.com/items?itemName=Vue.vscode-typescript-vue-plugin).

## Project Setup

```sh
npm install
```

### Link the @knime/scripting-editor dependency

To use local changes to the [`@knime/scripting-editor`](https://bitbucket.org/KNIME/knime-scripting-editor/src/master/org.knime.scripting.editor.js/) package it has to be linked with [`npm-link`](https://docs.npmjs.com/cli/v9/commands/npm-link).

### Compile and Hot-Reload for Development

If linked with `@knime/scripting-editor`, automatically build changes in `@knime/scripting-editor` with

```sh
npm run build-watch  # pwd: .../knime-scripting-editor/org.knime.scripting.editor.js/
```

#### Mocked Browser Preview

```sh
npm run dev:browser
```

#### KNIME Dialog Development Mode

Start KNIME Analytics Platform with the arguments

```
-Dchromium.remote_debugging_port=8988
-Dorg.knime.ui.dev.node.dialog.url=http://localhost:5173/
```

Run the development server

```sh
npm run dev:knime
```

### Type-Check, Compile and Minify for Production

```sh
npm run build
```

### Run Unit Tests with [Vitest](https://vitest.dev/)

```sh
npm run test:unit
```

### Lint with [ESLint](https://eslint.org/)

```sh
npm run lint
```
