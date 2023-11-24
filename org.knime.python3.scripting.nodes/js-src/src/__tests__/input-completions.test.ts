import * as monaco from "monaco-editor";
import { describe, it, expect, vi } from "vitest";
import { registerInputCompletions } from "../input-completions";

describe("registerInputCompletions", () => {
  it("registers input completions with Monaco editor", () => {
    const mockRegisterCompletionItemProvider = vi.fn();
    monaco.languages.registerCompletionItemProvider =
      mockRegisterCompletionItemProvider;

    registerInputCompletions([]);

    expect(mockRegisterCompletionItemProvider).toHaveBeenCalledWith("python", {
      provideCompletionItems: expect.any(Function),
    });
  });

  const simpleInputs = [
    { label: "input1", detail: "string" },
    { label: "input2", detail: "number" },
  ];

  const position = { lineNumber: 0, column: 0 };

  const getProvideCompletionItemsFn = (inputs: any) => {
    const mockRegisterCompletionItemProvider = vi.fn();
    monaco.languages.registerCompletionItemProvider =
      mockRegisterCompletionItemProvider;

    registerInputCompletions(inputs);

    return mockRegisterCompletionItemProvider.mock.calls[0][1]
      .provideCompletionItems;
  };

  const editorModelMock = (args?: {
    begVal?: string;
    endVal?: string;
    word?: string;
  }) => {
    const begVal = args?.begVal ?? "";
    const endVal = args?.endVal ?? begVal;
    const word = args?.word ?? "";
    return {
      getWordUntilPosition: () => ({ word, startColumn: 0, endColumn: 0 }),
      getValueInRange: ({ startColumn }: { startColumn: number }) =>
        startColumn === -1 ? begVal : endVal,
    };
  };

  it("provides input completions with quotes", () => {
    const provideCompletionsItems = getProvideCompletionItemsFn(simpleInputs);
    const items = provideCompletionsItems(editorModelMock(), position);
    expect(items.suggestions[0]).toEqual({
      label: "input1",
      detail: "string",
      insertText: '"input1"',
      sortText: "0000000000",
      kind: 0,
      range: {
        startColumn: 0,
        endColumn: 0,
        startLineNumber: 0,
        endLineNumber: 0,
      },
    });
    expect(items.suggestions[1]).toEqual({
      label: "input2",
      detail: "number",
      insertText: '"input2"',
      sortText: "0000000001",
      kind: 0,
      range: {
        startColumn: 0,
        endColumn: 0,
        startLineNumber: 0,
        endLineNumber: 0,
      },
    });
  });

  describe("add correct quotes", () => {
    it.each([
      { begVal: "", endVal: "", expBeg: '"', expEnd: '"' }, // no quotes
      { begVal: '"', endVal: '"', expBeg: "", expEnd: "" }, // double quotes
      { begVal: "'", endVal: "'", expBeg: "", expEnd: "" }, // single quotes
      { begVal: '"', endVal: "", expBeg: "", expEnd: '"' }, // beginning double quote
      { begVal: "'", endVal: "", expBeg: "", expEnd: "'" }, // beginning single quote
      { begVal: "", endVal: '"', expBeg: '"', expEnd: "" }, // ending double quote
      { begVal: "", endVal: "'", expBeg: "'", expEnd: "" }, // ending single quote
      { begVal: '"', endVal: "'", expBeg: "", expEnd: '"' }, // beginning double, ending single
      { begVal: "'", endVal: '"', expBeg: "", expEnd: "'" }, // beginning single, ending double
      { begVal: ".", endVal: "", expBeg: "", expEnd: "" }, // beginning dot
      { begVal: ".", endVal: "'", expBeg: "", expEnd: "" }, // beginning dot
    ])("start: $begVal, end: $endVal", ({ begVal, endVal, expBeg, expEnd }) => {
      const provideCompletionsItems = getProvideCompletionItemsFn(simpleInputs);
      const items = provideCompletionsItems(
        editorModelMock({ begVal, endVal }),
        position,
      );
      expect(items.suggestions[0]).toEqual(
        expect.objectContaining({
          insertText: `${expBeg}input1${expEnd}`,
        }),
      );
      expect(items.suggestions[1]).toEqual(
        expect.objectContaining({
          insertText: `${expBeg}input2${expEnd}`,
        }),
      );
    });
  });

  it("sorts suggestions based on the input", () => {
    const inputs = [...Array(100).keys()].map((i) => ({
      label: `input${i}`,
      detail: "string",
    }));
    const provideCompletionsItems = getProvideCompletionItemsFn(inputs);

    const items = provideCompletionsItems(editorModelMock(), position);
    for (let i = 0; i < 100; i++) {
      expect(items.suggestions[i]).toEqual(
        expect.objectContaining({
          sortText: `${i}`.padStart(10, "0"),
        }),
      );
    }
  });

  it("matches suggestions case-insensitive", () => {
    const inputs = [
      { label: "In1 and more text", detail: "string" },
      { label: "in2 and more text", detail: "number" },
    ];
    const provideCompletionsItems = getProvideCompletionItemsFn(inputs);

    const items = provideCompletionsItems(editorModelMock({ word: "in1" }), {
      lineNumber: 1,
      column: 2,
    });
    expect(items.suggestions[0]).toEqual(
      expect.objectContaining({ label: "In1 and more text" }),
    );
  });
});
