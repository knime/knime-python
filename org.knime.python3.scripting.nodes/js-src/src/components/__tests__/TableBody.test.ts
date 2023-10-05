import { describe, expect, it, vi } from "vitest";
import { mount } from "@vue/test-utils";
import { useWorkspaceStore, type WorkspaceStore } from "@/store";
import { pythonScriptingService } from "@/python-scripting-service";
import { getScriptingService } from "@knime/scripting-editor";

const mockStore = {
  workspace: [
    {
      name: "Hallo",
      value: "1",
      type: "Int",
    },
  ],
} as WorkspaceStore;

vi.mock("@/store");
import TableBody from "../TableBody.vue";

describe("TableBody.vue", () => {
  it("test default props", () => {
    vi.mocked(useWorkspaceStore).mockReturnValueOnce(mockStore);
    const body = mount(TableBody, {
      propsData: {},
    });
    expect(body.props().columnWidths).toStrictEqual([100, 100, 100]);
    expect(body.find("tbody")).toBeTruthy();
    const firstRow = body.find("tr");
    expect(firstRow.exists()).toBeTruthy();
  });

  it("test none empty store", () => {
    vi.mocked(useWorkspaceStore).mockReturnValueOnce(mockStore);
    const body = mount(TableBody, {
      propsData: { columnWidths: [101, 100, 101] },
    });
    expect(body.props().columnWidths).toStrictEqual([101, 100, 101]);
    expect(body.find("tbody")).toBeTruthy();
    const firstRow = body.find("tr");
    expect(firstRow.exists()).toBeTruthy();
  });

  it("test empty store", () => {
    vi.mocked(useWorkspaceStore).mockReturnValueOnce({});
    const body = mount(TableBody, {
      propsData: { columnWidths: [100, 100, 100] },
    });
    expect(body.props().columnWidths).toStrictEqual([100, 100, 100]);
    expect(body.find("tbody")).toBeTruthy();
    const firstRow = body.find("tr");
    expect(firstRow.exists()).toBeFalsy();
  });

  it("button click on table calls printVariable", () => {
    const name = "testName";

    vi.mocked(useWorkspaceStore).mockReturnValueOnce({
      workspace: [{ name, value: "test", type: "test" }],
    });

    // let sendToService return smth
    vi.mocked(getScriptingService().sendToService).mockImplementation(() => {
      return Promise.resolve({
        status: "SUCCESS",
        description: "mocked execution info",
      });
    });

    const body = mount(TableBody);

    // check table is added
    const tableRows = body.findAll("tr");
    expect(tableRows.length).toBe(1);

    // find table row
    const tableRow = tableRows[0];
    expect(tableRow).toBeDefined();

    // check click event
    const runScript = vi.spyOn(pythonScriptingService, "printVariable");
    tableRow.trigger("click");
    expect(runScript).toHaveBeenCalledWith(name);
    expect(getScriptingService().sendToService).toHaveBeenCalledWith(
      "runInExistingSession",
      [`print(""">>> print(${name})\n""" + str(${name}))`],
    );
  });
});
