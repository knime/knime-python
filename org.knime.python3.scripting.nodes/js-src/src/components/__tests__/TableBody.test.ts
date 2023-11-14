import { pythonScriptingService } from "@/python-scripting-service";
import { useWorkspaceStore, type WorkspaceStore } from "@/store";
import { getScriptingService } from "@knime/scripting-editor";
import { mount } from "@vue/test-utils";
import { describe, expect, it, vi } from "vitest";
import TableBody from "../TableBody.vue";

const mockStore = {
  workspace: [
    {
      name: "Hallo",
      type: "Int",
      value: "1",
    },
    {
      name: "Foo",
      type: "String",
      value: "bar",
    },
  ],
} as WorkspaceStore;

vi.mock("@/store");

describe("TableBody.vue", () => {
  const doMount = (columnWidths?: [number, number, number]) => {
    return mount(TableBody, {
      propsData: { columnWidths: columnWidths ?? [100, 100, 100] },
    });
  };

  it("should apply column widths", () => {
    vi.mocked(useWorkspaceStore).mockReturnValueOnce(mockStore);
    const body = doMount([110, 120, 130]);
    const tableCells = body.findAll("td");
    expect(tableCells.length).toBe(6);
    expect(tableCells[0].element.style.width).toBe("110px");
    expect(tableCells[1].element.style.width).toBe("120px");
    expect(tableCells[2].element.style.width).toBe("130px");
  });

  it("should render temporary values from store", () => {
    vi.mocked(useWorkspaceStore).mockReturnValueOnce(mockStore);
    const body = doMount();
    expect(body.find("tbody")).toBeTruthy();
    const tableRows = body.findAll("tr");
    expect(tableRows.length).toBe(2);

    const firstRow = tableRows[0];
    expect(firstRow.exists()).toBeTruthy();
    const tableCells0 = firstRow.findAll("td");
    expect(tableCells0.length).toBe(3);
    expect(tableCells0[0].text()).toBe("Hallo");
    expect(tableCells0[1].text()).toBe("Int");
    expect(tableCells0[2].text()).toBe("1");

    const secondRow = tableRows[1];
    expect(secondRow.exists()).toBeTruthy();
    const tableCells1 = secondRow.findAll("td");
    expect(tableCells1.length).toBe(3);
    expect(tableCells1[0].text()).toBe("Foo");
    expect(tableCells1[1].text()).toBe("String");
    expect(tableCells1[2].text()).toBe("bar");
  });

  it("should not render any rows if no temporary values", () => {
    vi.mocked(useWorkspaceStore).mockReturnValueOnce({});
    const body = doMount();
    expect(body.find("tbody")).toBeTruthy();
    const firstRow = body.find("tr");
    expect(firstRow.exists()).toBeFalsy();
  });

  it("should call printVariable on table row click", () => {
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

    const body = doMount();
    const tableRow = body.find("tr");
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
