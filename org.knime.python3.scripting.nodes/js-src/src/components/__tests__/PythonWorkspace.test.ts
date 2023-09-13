import { flushPromises, mount } from "@vue/test-utils";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import PythonWorkspace from "../PythonWorkspace.vue";
import { getScriptingService } from "@knime/scripting-editor";
import Button from "webapps-common/ui/components/Button.vue";
import { useWorkspaceStore } from "@/store";
import { pythonScriptingService } from "@/python-scripting-service";

describe("PythonWorkspace", () => {
  const doMount = async ({ props } = { props: {} }) => {
    vi.mocked(getScriptingService().sendToService).mockImplementation(() => {
      return Promise.resolve({
        status: "SUCCESS",
        description: "mocked execution info",
      });
    });
    const wrapper = mount(PythonWorkspace, { props });
    await flushPromises();
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

  it("renders restart button", async () => {
    const { wrapper } = await doMount();
    expect(wrapper.find(".restart-button").exists()).toBeTruthy();
  });

  it("restarts python session when restart button is clicked", async () => {
    const { wrapper } = await doMount();
    await flushPromises();
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
    const { wrapper } = await doMount();
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

    const { wrapper } = await doMount();
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

  it("reset button enabled if inputs are available", async () => {
    const { wrapper } = await doMount();
    const button = wrapper.findComponent(Button);
    expect(button.props().disabled).toBeFalsy();
  });

  it("reset button disabled if inputs are not available", async () => {
    vi.mocked(getScriptingService().inputsAvailable).mockImplementation(() => {
      return Promise.resolve(false);
    });

    const { wrapper } = await doMount();
    const button = wrapper.findComponent(Button);
    expect(button.props().disabled).toBeTruthy();
  });
});
