<script setup lang="ts">
import Button from "webapps-common/ui/components/Button.vue";
import { pythonScriptingService } from "../python-scripting-service";
import { handleSessionInfo } from "./utils/handleSessionInfo";

const restartPythonSession = async () => {
  handleSessionInfo(
    await pythonScriptingService.killInteractivePythonSession(),
  );
  handleSessionInfo(
    await pythonScriptingService.startInteractivePythonSession(),
  );
};
</script>

<template>
  <div class="workspace">
    <Button
      class="restart-button"
      compact
      primary
      @click="restartPythonSession"
    >
      <div>Restart session</div>
    </Button>
  </div>
</template>

<style scoped lang="postcss">
.workspace {
  display: flex;
  flex-direction: column;
  justify-content: center;
  align-items: center;
}

.restart-button {
  margin: 10px;
  min-width: 80px;
}
</style>
./utils/handleExecutionInfo
