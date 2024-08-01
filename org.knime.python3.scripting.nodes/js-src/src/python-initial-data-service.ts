import {
  getInitialDataService,
  type InputOutputModel,
  type KAIConfig,
  type PortConfigs,
} from "@knime/scripting-editor";
import type { ExecutableOption } from "./types/common";

export type PythonInitialData = {
  inputObjects: InputOutputModel[];
  inputPortConfigs: PortConfigs;
  inputsAvailable: boolean;
  flowVariables: InputOutputModel;
  outputObjects: InputOutputModel[];
  hasPreview: boolean;
  executableOptionsList: ExecutableOption[];
  kAiConfig: KAIConfig;
};

export const getPythonInitialDataService = () => ({
  ...getInitialDataService(),
  getInitialData: async () =>
    (await getInitialDataService().getInitialData()) as PythonInitialData,
});
