import { ScriptingService,
    muteReactivity,
    createJsonServiceAndLoadSettings,
    NodeSettings } from 'scripting-editor/src/utils/scripting-service';

export type Workspace = { names: string[]; types: string[]; values: string[] };
export type InputObjects = string[][];

// TODO(AP-19331) Add Conda environment selection
export type PythonNodeSettings = NodeSettings;

export class PythonScriptingService extends ScriptingService<PythonNodeSettings> {
    startInteractive(): Promise<void> {
        return this.sendToService('startInteractive');
    }

    runInteractive(script: string): Promise<string> {
        return this.sendToService('runInteractive', [script]);
    }

    getInputObjects(): Promise<string[][]> {
        return this.sendToService('getInputObjects');
    }
}

export const createScriptingService = async () => {
    const { jsonDataService, initialNodeSettings } = await createJsonServiceAndLoadSettings();
    const scriptingService = new PythonScriptingService(jsonDataService, initialNodeSettings);
    muteReactivity(scriptingService);
    return scriptingService;
};
