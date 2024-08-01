import type {
  ExecutableOption,
  PythonScriptingNodeSettings,
} from "./types/common";
import type {
  InputOutputModel,
  PortConfigs,
  KAIConfig,
} from "@knime/scripting-editor";

export type PythonInitialData = {
  settings: PythonScriptingNodeSettings;
  inputObjects: InputOutputModel[];
  inputPortConfigs: PortConfigs;
  inputsAvailable: boolean;
  flowVariables: InputOutputModel;
  outputObjects: InputOutputModel[];
  hasPreview: boolean;
  executableOptionsList: ExecutableOption[];
  initialLanguageServerConfig: any;
  kAiConfig: KAIConfig;
};
