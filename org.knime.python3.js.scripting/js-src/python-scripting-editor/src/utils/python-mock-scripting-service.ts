/* eslint-disable @typescript-eslint/no-unused-vars */
/* eslint-disable class-methods-use-this */
import type { ExecutableOption,
    InputTableInfo,
    PythonNodeSettings,
    CondaPackageInfo,
    InputPortInfo,
    ExecutableInfo,
    PythonScriptingService } from './python-scripting-service';

import type { FlowVariableSetting } from '@knime/ui-extension-service';

import type { FlowVariableInput, ConsoleText } from 'scripting-editor/src/utils/scripting-service';


class PythonScriptingServiceMock implements PythonScriptingService {
    protected readonly flowVariableSettings: {[key: string]: FlowVariableSetting}; // TODO(UIEXT-479) refactor how flow variable information are provided
    // protected readonly initialNodeSettings: T;
    protected currentNodeSettings: PythonNodeSettings;
    protected initialNodeSettings: PythonNodeSettings;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    eventHandlers: { [type: string]: (args: any) => void };

    constructor({
        flowVariableSettings,
        initialNodeSettings,
    }: {
        flowVariableSettings: {[key: string]: FlowVariableSetting};
        initialNodeSettings: PythonNodeSettings
    }) {
        this.flowVariableSettings = flowVariableSettings;
        this.initialNodeSettings = initialNodeSettings;
        this.eventHandlers = {};
        this.currentNodeSettings = {
            ...initialNodeSettings,
        };
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    protected registerEventHandler(type: string, handler: (args: any) => void) {
        this.eventHandlers[type] = handler;
    }

    dialogOpened(): Promise<void> {
        return new Promise((r) => setTimeout(r, 100));
    }
    
    initExecutableOptions(): Promise<void> {
        //
        return new Promise((r) => setTimeout(r, 100));
    }

    sendLastConsoleOutput(): Promise<void> {
        return new Promise((r) => setTimeout(r, 100));
    }

    startInteractive(executableSelection: string): Promise<void> {
        return new Promise((r) => setTimeout(r, 100));
    }

    getFlowVariableInputs() {
        return new Promise((r) => setTimeout(r, 100)).then(() => [
            { name: 'MyFlowVariable', value: 'CustomFlowVariable' },
            { name: 'MyFlow', value: '1' },
        ]);
    }

    runInteractive(script: string): Promise<string> {
        // Trigger both Console and Workspace change.
        const res = {
            names: ['x'],
            types: ['int'],
            values: ['3'],
        };
        this.eventHandlers.console({
            text: 'x=3',
            stderr: false,
        } as ConsoleText);
        return new Promise((r) => setTimeout(r, 100)).then(() => JSON.stringify(
            res,
        ));
    }

    getInputObjects(): Promise<InputPortInfo[]> {
        return new Promise((r) => setTimeout(r, 100)).then(() => [
            {
                variableName: 'MockTable',
                type: 'table',
                columnNames: ['T1'],
                columnTypes: ['int'],
            } as InputTableInfo,
        ]);
    }

    applySettings(): Promise<any> {
        return new Promise((r) => setTimeout(r, 100));
    }

    getInitialExecutableSelection(): string {
        return this.initialNodeSettings.executableSelection;
    }

    getExecutableSelection(): string {
        return this.currentNodeSettings.executableSelection;
    }

    setExecutableSelection(id: string) {
        this.currentNodeSettings.executableSelection = id;
    }

    getInitialScript(): string {
        return 'import numpy as np\n';
    }

    getScript(): string {
        return this.currentNodeSettings.script;
    }

    setScript(script: string) {
        this.currentNodeSettings.script = script;
    }

    sendLanguageServerMessage(message: string): Promise<any> {
        return new Promise((r) => setTimeout(r, 100));
    }

    registerLanguageServerEventHandler(handler: (message: string) => void) {
        this.registerEventHandler('language-server', handler);
    }

    startLanguageClient(name: string, documentSelector?: string[]) : Promise<any> {
        return new Promise((r) => setTimeout(r, 100));
    }

    getExecutableOptions(executableSelection: string): Promise<ExecutableOption[]> {
        return Promise.resolve([{
            type: 'STRING_VAR',
            id: '1',
            pythonExecutable: '/home/',
            condaEnvName: 'hallo',
            condaEnvDir: '/home/',
        } as ExecutableOption]);
    }

    getExecutableInfo(id: string): Promise<ExecutableInfo> {
        return Promise.resolve({
            pythonVersion: '3.10',
            packages: [{
                name: 'name',
                version: 'version',
                build: 'build',
                channel: 'channel',
            } as CondaPackageInfo],
        } as ExecutableInfo);
    }

    registerConsoleEventHandler(handler: (text: ConsoleText) => void) {
        this.eventHandlers.console = (e) => new Promise((r) => setTimeout(r, 100)).then(() => {
            handler(e);
        });
    }
}

export const createScriptingService = async () => {
    await new Promise((r) => setTimeout(r, 1000));
    const scriptingService = new PythonScriptingServiceMock({
        flowVariableSettings: {},
        initialNodeSettings: {
            script: '#This is mocking example',
            executableSelection: '1',
        },
    });
    return scriptingService as PythonScriptingService;
};
