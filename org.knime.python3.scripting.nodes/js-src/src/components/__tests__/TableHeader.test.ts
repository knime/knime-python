import { mount } from "@vue/test-utils";
import { beforeAll, describe, expect, it, vi } from "vitest";
import TableHeader, {
  type ColumnSizes,
  type ColumnState,
  type MouseTracker,
} from "../TableHeader.vue";

type TableHeaderState = {
  columnState: ColumnState;
  mouseState: MouseTracker;
  columnSizes?: ColumnSizes;
};

describe("TableHeader.vue", () => {
  const doMount = (containerWidth = 60) => {
    const wrapper = mount(TableHeader, { propsData: { containerWidth } });
    return { wrapper, state: wrapper.vm as any as TableHeaderState };
  };

  beforeAll(() => {
    window.PointerEvent = {} as any;
    window.HTMLElement.prototype.releasePointerCapture = vi.fn();
    window.HTMLElement.prototype.setPointerCapture = vi.fn();
  });

  it("should update mouseState on pointermove", async () => {
    const { wrapper, state } = doMount();
    state.columnState.dragIndex = 0;

    const handle = wrapper.find({ ref: "dragHandle-0" });
    await handle.trigger("pointermove", { clientX: 100 });
    expect(state.mouseState.currentX).toBe(100);
  });

  it("should update mouseState on pointerover", async () => {
    const { wrapper, state } = doMount();
    const handle = wrapper.find({ ref: "dragHandle-1" });
    state.columnState.dragIndex = null;
    await handle.trigger("pointerover");
    expect(state.mouseState.hoverIndex).toBe(1);

    state.columnState.dragIndex = 1;
    state.mouseState.hoverIndex = null;
    await handle.trigger("pointerover");
    expect(state.mouseState.hoverIndex).toBeNull();
  });

  it("should update mouseState on pointerleave", async () => {
    const { wrapper, state } = doMount();
    const handle = wrapper.find({ ref: "dragHandle-1" });
    state.mouseState.hoverIndex = 1;

    state.columnState.dragIndex = null;
    await handle.trigger("pointerleave");
    expect(state.mouseState.hoverIndex).toBeNull();
  });

  it("should start dragging on pointerdown", async () => {
    const { wrapper, state } = doMount();
    const handle = wrapper.find({ ref: "dragHandle-1" });
    const columnIndex = 1;
    const clientX = 100;

    state.mouseState.hoverIndex = 1;
    state.columnState.dragIndex = null;
    await handle.trigger("pointerdown", { pointerId: 0, clientX });
    expect(state.columnState.dragIndex).toBe(columnIndex);
    expect(window.HTMLElement.prototype.setPointerCapture).toHaveBeenCalledWith(
      0,
    );
    expect(state.columnState.pageXOnDragStart).toBe(clientX);
  });

  it("should stop dragging on pointerup", async () => {
    const { wrapper, state } = doMount();
    const handle = wrapper.find({ ref: "dragHandle-1" });

    state.columnState.dragIndex = 1;

    state.columnState.pageXOnDragStart = 1;
    await handle.trigger("pointerup", { pointerId: 0 });
    expect(state.columnState.pageXOnDragStart).toBeNull();
    expect(state.columnState.dragIndex).toBeNull();
    expect(
      window.HTMLElement.prototype.releasePointerCapture,
    ).toHaveBeenCalledWith(0);
  });

  it("should stop dragging on lostpointercapture", async () => {
    const { wrapper, state } = doMount();

    const handle = wrapper.find({ ref: "dragHandle-1" });

    state.columnState.dragIndex = 1;
    state.mouseState.hoverIndex = 1;
    state.columnState.pageXOnDragStart = 1;
    await handle.trigger("lostpointercapture", { pointerId: 0 });
    expect(state.columnState.dragIndex).toBeNull();
    expect(state.mouseState.hoverIndex).toBeNull();
    expect(
      window.HTMLElement.prototype.releasePointerCapture,
    ).toHaveBeenCalledWith(0);
  });

  describe("column size computation", () => {
    it("should update sizes when dragging second handle to the right", async () => {
      const { wrapper, state } = doMount(120);
      const handle = wrapper.find({ ref: "dragHandle-1" });
      state.columnState.dragIndex = 1;
      state.mouseState.currentX = 30;

      await handle.trigger("pointermove", { clientX: 40 });
      expect(state.columnSizes).toStrictEqual([40, 50, 30]);
    });

    it("should update sizes when dragging second handle to the left", async () => {
      const { wrapper, state } = doMount(120);
      const handle = wrapper.find({ ref: "dragHandle-1" });
      state.columnState.dragIndex = 1;
      state.mouseState.currentX = 80;

      await handle.trigger("pointermove", { clientX: 40 });
      expect(state.columnSizes).toStrictEqual([30, 30, 60]);

      await wrapper.setProps({ containerWidth: 99 });
      expect(state.columnSizes).toStrictEqual([33, 33, 33]);

      // Less than minimal size
      await wrapper.setProps({ containerWidth: 210 });
      expect(state.columnSizes).toStrictEqual([70, 70, 70]);

      state.mouseState.currentX = 80;
      await handle.trigger("pointermove", { clientX: 40 });
      expect(state.columnSizes).toStrictEqual([70, 30, 110]);

      await wrapper.setProps({ containerWidth: 40 });
      expect(state.columnSizes).toStrictEqual([30, 30, 30]);
    });

    it("should update sizes when dragging first handle", async () => {
      const { wrapper, state } = doMount(120);
      const handle = wrapper.find({ ref: "dragHandle-0" });
      state.columnState.dragIndex = 0;
      state.mouseState.currentX = 80;

      await handle.trigger("pointermove", { clientX: 40 });
      expect(state.columnSizes).toStrictEqual([30, 50, 40]);

      await wrapper.setProps({ containerWidth: 99 });
      expect(state.columnSizes).toStrictEqual([33, 33, 33]);

      // Less than minimal size
      await wrapper.setProps({ containerWidth: 210 });
      expect(state.columnSizes).toStrictEqual([70, 70, 70]);

      state.mouseState.currentX = 80;
      await wrapper.setProps({ containerWidth: 210 });
      await handle.trigger("pointermove", { clientX: 200 });
      expect(state.columnSizes).toStrictEqual([150, 30, 30]);

      state.mouseState.currentX = 80;
      await wrapper.setProps({ containerWidth: 210 });
      await handle.trigger("pointermove", { clientX: 120 });
      expect(state.columnSizes).toStrictEqual([150, 30, 30]);

      // Less than minimal size
      await wrapper.setProps({ containerWidth: 40 });
      expect(state.columnSizes).toStrictEqual([30, 30, 30]);
    });

    it("should resize all columns on increased container width", async () => {
      const { wrapper, state } = doMount(120);
      expect(state.columnSizes).toStrictEqual([40, 40, 40]);

      await wrapper.setProps({ containerWidth: 150 });
      expect(state.columnSizes).toStrictEqual([50, 50, 50]);
    });
  });
});
