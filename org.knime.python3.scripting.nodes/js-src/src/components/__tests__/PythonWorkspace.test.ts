import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import PythonWorkspace from "../PythonWorkspace.vue";
import { getScriptingService } from "@knime/scripting-editor";

import { useWorkspaceStore } from "@/store";
import { pythonScriptingService } from "@/python-scripting-service";

describe("PythonWorkspace", () => {
  const doMount = ({ props } = { props: {} }) => {
    vi.mocked(getScriptingService().sendToService).mockImplementation(() => {
      return Promise.resolve({
        status: "SUCCESS",
        description: "mocked execution info",
      });
    });
    const wrapper = mount(PythonWorkspace, { props });
    return { wrapper };
  };

  beforeEach(() => {
    const store = useWorkspaceStore();
    store.workspace = undefined;
    vi.useFakeTimers();
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.clearAllTimers();
  });

  it("renders restart button", () => {
    const { wrapper } = doMount();
    expect(wrapper.find(".restart-button").exists()).toBeTruthy();
  });

  it("restarts python session when restart button is clicked", async () => {
    const { wrapper } = doMount();
    const sendToServiceSpy = vi.spyOn(getScriptingService(), "sendToService");
    await wrapper.find(".restart-button").trigger("click");
    vi.runAllTimers();
    await flushPromises();
    expect(sendToServiceSpy).toHaveBeenNthCalledWith(1, "killSession");
    expect(sendToServiceSpy).toHaveBeenNthCalledWith(
      2,
      "startInteractive",
      expect.anything(),
    );
  });

  it("button click should reset store", async () => {
    const { wrapper } = doMount();
    const button = wrapper.find(".restart-button");
    const store = useWorkspaceStore();
    expect(store.workspace).toBeUndefined();
    store.workspace = [{ name: "test", value: "test", type: "test" }];
    expect(store.workspace).toBeDefined();
    button.trigger("click");
    await flushPromises();
    expect(store.workspace).toStrictEqual([]);
  });

  it("button click on table calls runScript", async () => {
    const store = useWorkspaceStore();
    store.workspace = [{ name: "testName", value: "test", type: "test" }];

    const { wrapper } = doMount();
    const table = wrapper.find("table");

    await flushPromises();

    // check table is added
    const tableRows = table.findAll("tr");
    expect(tableRows.length).toBe(1);

    // find table row
    const tableRow = tableRows[0];
    const keyToFind = store.workspace[0].name;
    const valueToFind = store.workspace[0].value;
    const name = tableRow.find(`td[${keyToFind}="${valueToFind}"]`);
    expect(name).toBeDefined();

    // check click event
    const runScript = vi.spyOn(pythonScriptingService, "runScript");
    tableRow.trigger("click");
    expect(runScript).toHaveBeenCalledWith(`print(${keyToFind})`);
  });
});
