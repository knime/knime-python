export type ExecutableOptionType =
  | "PREF_BUNDLED"
  | "PREF_CONDA"
  | "PREF_MANUAL"
  | "CONDA_ENV_VAR"
  | "STRING_VAR"
  | "MISSING_VAR";

export type ExecutableOption = {
  type: ExecutableOptionType;
  id: string;
  pythonExecutable: string;
  condaEnvName: string;
  condaEnvDir: string;
};

export type PythonScriptingNodeSettings = {
  script: string;
  executableSelection: string;
  scriptUsedFlowVariable?: string;
  settingsAreOverriddenByFlowVariable?: boolean;
};

export type Workspace = {
  name: string;
  type: string;
  value: string;
}[];

export type ExecutionResult =
  | "SUCCESS"
  | "EXECUTION_ERROR"
  | "KNIME_ERROR"
  | "FATAL_ERROR"
  | "CANCELLED";

export type ExecutionInfo = {
  status: ExecutionResult;
  description: string;
  traceback?: string[];
  data?: Workspace;
  hasValidView?: boolean;
};

export type KillSessionInfo = {
  status: "SUCCESS" | "ERROR";
  description: string;
};
