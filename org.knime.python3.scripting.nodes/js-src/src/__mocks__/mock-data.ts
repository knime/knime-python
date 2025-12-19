import { createSettingsServiceMock } from "@knime/scripting-editor/settings-service-browser-mock";

import type { PythonInitialData } from "@/python-scripting-service";
import type { PythonScriptingNodeSettings } from "@/types/common";

export const DEFAULT_INITIAL_SETTINGS: PythonScriptingNodeSettings = {
  script: "mocked python script (from browser mock)",
  executableSelection: "",
};

export const DEFAULT_INITIAL_DATA: PythonInitialData = {
  hasPreview: true,
  executableOptionsList: [],
  inputObjects: [
    {
      name: "Input Table 1",
      codeAlias: "knio.input_tables[0].to_pandas()",
      requiredImport: "import knio.scripting.io as knio",
      multiSelection: true,
      subItemCodeAliasTemplate: `knio.input_tables[0][
        {{~#if subItems.[1]~}}
          [{{#each subItems}}"{{{escapeDblQuotes this}}}"{{#unless @last}},{{/unless}}{{/each}}]
        {{~else~}}
          "{{{escapeDblQuotes subItems.[0].name}}}"
          {{~/if~}}
      ].to_pandas()`,
    },
    {
      name: "Input Table 2",
      codeAlias: "knio.input_tables[1].to_pandas()",
      requiredImport: "import knio.scripting.io as knio",
      multiSelection: true,
      subItemCodeAliasTemplate: `knio.input_tables[1][
        {{~#if subItems.[1]~}}
          [{{#each subItems}}"{{{escapeDblQuotes this.name}}}"{{#unless @last}},{{/unless}}{{/each}}]
        {{~else~}}
          "{{{escapeDblQuotes subItems.[0].name}}}"
          {{~/if~}}
      ].to_pandas()`,
      subItems: [
        {
          name: "Foo",
          type: { displayName: "Number" },
          supported: true,
        },
        {
          name: 'Bar & "<xml>"',
          type: { displayName: "String" },
          supported: false,
        },
      ],
      portType: "table",
    },
    {
      name: "Input Object 1",
      portType: "object",
      portIconColor: "#FF0000",
    },
  ],
  outputObjects: [
    {
      name: "Output Table 1",
      codeAlias: "knio.output_tables[0]",
      requiredImport: "import knio.scripting.io as knio",
    },
    {
      name: "Output Object 1",
      portType: "object",
      portIconColor: "#FF0000",
    },
    {
      name: "Output View",
      portType: "view",
    },
  ],
  flowVariables: {
    name: "Flow Variable 1",
    codeAlias: "knio.flow_variables",
    requiredImport: "import knio.scripting.io as knio",
    multiSelection: false,
    subItemCodeAliasTemplate:
      'knio.flow_variables["{{{escapeDblQuotes subItems.[0].name}}}"]',
    subItems: [
      {
        name: "Flow var 1",
        type: { displayName: "Number" },
        supported: true,
      },
      {
        name: 'Bar & "<xml>"',
        type: { displayName: "Something weird" },
        supported: false,
      },
    ],
  },
  kAiConfig: {
    isKaiEnabled: true,
    hubId: "my mocked KNIME hub id",
  },
  inputConnectionInfo: [
    // flow variable port
    { status: "OK", isOptional: true },
    // input port 1
    { status: "OK", isOptional: false },
  ],
  inputPortConfigs: {
    inputPorts: [
      {
        nodeId: "id0",
        portIdx: 0,
        portName: "Input 1",
        portViewConfigs: [
          {
            label: "Input 1",
            portViewIdx: 0,
          },
        ],
      },
    ],
  },
};

export const createPythonSettingsServiceMock = (
  settings?: PythonScriptingNodeSettings,
) =>
  createSettingsServiceMock({ settings: settings ?? DEFAULT_INITIAL_SETTINGS });
