import { defineStore } from "pinia"
import type { Workspace } from "./types/common";

export const useWorkspaceStore = defineStore("workspace", {
  state: (): { workspace: Workspace } => {
    return { workspace: [] }
  },
  actions: {
    reset() {
      this.workspace = [];
    },
    update(workspace: Workspace) {
      this.workspace = workspace;
    },
  },
})