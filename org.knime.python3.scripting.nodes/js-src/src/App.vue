<script setup lang="ts">
import { CodeEditor } from "@knime/scripting-editor";
</script>

<template>
  <main>
    <div style="height: 600px">
      <CodeEditor initial-script="foo" language="python" />
    </div>
  </main>
</template>
