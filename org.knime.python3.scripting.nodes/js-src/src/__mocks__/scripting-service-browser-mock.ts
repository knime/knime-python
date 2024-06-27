import {
  createScriptingServiceMock,
  DEFAULT_FLOW_VARIABLE_INPUTS,
  DEFAULT_INPUT_OBJECTS,
} from "@knime/scripting-editor/scripting-service-browser-mock";
import { getScriptingService } from "@knime/scripting-editor";

if (import.meta.env.MODE === "development.browser") {
  const scriptingService = createScriptingServiceMock({
    sendToServiceMockResponses: {
      getLanguageServerConfig: () => Promise.resolve(JSON.stringify({})),
      hasPreview: () => Promise.resolve(true),
      getExecutableOptionsList: () => Promise.resolve([]),
    },
    initialSettings: {
      script:
        "print('Hello World!')\n\nprint('Hello World!')\n\nprint('Hello World!')\n",
    },
    inputObjects: [
      {
        ...DEFAULT_INPUT_OBJECTS[0],
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
    flowVariableInputs: {
      ...DEFAULT_FLOW_VARIABLE_INPUTS,
      codeAlias: "knio.flow_variables",
      requiredImport: "import knio.scripting.io as knio",
      multiSelection: false,
      subItemCodeAliasTemplate:
        'knio.flow_variables["{{{escapeDblQuotes subItems.[0]}}}"]',
    },
  });

  Object.assign(getScriptingService(), scriptingService);
}
