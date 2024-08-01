import type { PythonScriptingNodeSettings } from "@/types/common";
import type { GenericInitialData } from "@knime/scripting-editor";

export const DEFAULT_INITIAL_DATA: GenericInitialData = {
  settings: {
    script: "",
    executableSelection: "",
  } satisfies PythonScriptingNodeSettings,
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
          "{{{escapeDblQuotes subItems.[0]}}}"
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
          [{{#each subItems}}"{{{escapeDblQuotes this}}}"{{#unless @last}},{{/unless}}{{/each}}]
        {{~else~}}
          "{{{escapeDblQuotes subItems.[0]}}}"
          {{~/if~}}
      ].to_pandas()`,
      subItems: [
        {
          name: "Foo",
          type: "Number",
        },
        {
          name: 'Bar & "<xml>"',
          type: "String",
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
      'knio.flow_variables["{{{escapeDblQuotes subItems.[0]}}}"]',
  },
  kAiConfig: {
    codeAssistantEnabled: true,
    codeAssistantInstalled: true,
    hubId: "my mocked KNIME hub id",
    loggedIn: true,
  },
  inputsAvailable: true,
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
