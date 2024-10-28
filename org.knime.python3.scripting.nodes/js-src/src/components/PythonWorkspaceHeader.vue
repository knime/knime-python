<script setup lang="ts">
import { onMounted, reactive, watch } from "vue";

export type ColumnSizes = [number, number, number];

const MIN_COLUMN_SIZE = 30;
const columnSizes = reactive<ColumnSizes>([0, 0, 0]);

const props = defineProps({
  containerWidth: {
    type: Number,
    // eslint-disable-next-line no-magic-numbers
    default: () => 120,
  },
});

const columnHeaders = ["Name", "Type", "Value"];

const emit = defineEmits<{
  (e: "updateHeaderWidths", value: ColumnSizes): void;
}>();

export type MouseTracker = {
  hoverIndex: number | null;
  currentX: number | null;
};

export type ColumnState = {
  dragIndex: number | null;
  columnIndex: number | null;
  pageXOnDragStart: number | null; // the x coordinate at which the mouse was clicked when starting the resize drag
};

const mouseState = reactive<MouseTracker>({
  hoverIndex: null,
  currentX: null,
});

const columnState = reactive<ColumnState>({
  dragIndex: null,
  columnIndex: null,
  pageXOnDragStart: null,
});

onMounted(() => {
  columnSizes[0] = Math.max(
    props.containerWidth / columnHeaders.length,
    MIN_COLUMN_SIZE,
  );
  columnSizes[1] = Math.max(
    props.containerWidth / columnHeaders.length,
    MIN_COLUMN_SIZE,
  );
  columnSizes[2] = Math.max(
    props.containerWidth / columnHeaders.length,
    MIN_COLUMN_SIZE,
  );
  emit("updateHeaderWidths", columnSizes);
});

watch(
  () => props.containerWidth,
  (newSize) => {
    columnSizes[0] = Math.max(newSize / columnHeaders.length, MIN_COLUMN_SIZE);
    columnSizes[1] = Math.max(newSize / columnHeaders.length, MIN_COLUMN_SIZE);
    columnSizes[2] = Math.max(newSize / columnHeaders.length, MIN_COLUMN_SIZE);
    emit("updateHeaderWidths", columnSizes);
  },
);

const resizeColumns = (delta: number) => {
  if (columnState.dragIndex === 1) {
    // right handle
    if (delta > 0) {
      // to the right
      // possible that right side is saturated with delta or not.
      columnSizes[2] = Math.max(columnSizes[2] - delta, MIN_COLUMN_SIZE);
      columnSizes[1] = props.containerWidth - columnSizes[0] - columnSizes[2];
    } else if (columnSizes[1] + delta >= MIN_COLUMN_SIZE) {
      // left drag
      columnSizes[2] -= delta;
      columnSizes[1] += delta;
    } else {
      // not enough space
      // set to minimum
      columnSizes[1] = MIN_COLUMN_SIZE;
      let difference =
        props.containerWidth - (MIN_COLUMN_SIZE - delta + columnSizes[2]);
      columnSizes[0] = Math.max(difference, MIN_COLUMN_SIZE);
      columnSizes[2] = props.containerWidth - columnSizes[0] - columnSizes[1];
    }
  } else if (columnState.dragIndex === 0) {
    // left handle
    if (delta < 0) {
      columnSizes[0] = Math.max(columnSizes[0] + delta, MIN_COLUMN_SIZE);
      columnSizes[1] = props.containerWidth - columnSizes[0] - columnSizes[2];
    } else if (columnSizes[1] - delta >= MIN_COLUMN_SIZE) {
      // left drag
      // symmetric
      columnSizes[0] += delta;
      columnSizes[1] -= delta;
    } else {
      // set to minimum
      columnSizes[1] = MIN_COLUMN_SIZE;
      let difference =
        props.containerWidth - (MIN_COLUMN_SIZE + delta + columnSizes[0]);
      columnSizes[2] = Math.max(difference, MIN_COLUMN_SIZE);
      columnSizes[0] = props.containerWidth - columnSizes[1] - columnSizes[2];
    }
  }
  emit("updateHeaderWidths", columnSizes);
};

const onLostPointerCapture = (event: any) => {
  // reset
  columnState.dragIndex = null;
  mouseState.hoverIndex = null;
  event.target!.onpointermove = null;
  event.target.releasePointerCapture(event.pointerId);
};

const onPointerOver = (_: any, columnIndex: number) => {
  // needed to drag over other handles
  if (columnState.dragIndex === null) {
    mouseState.hoverIndex = columnIndex;
  }
};

const onPointerLeave = () => {
  // needed to drag over other handles
  if (columnState.dragIndex === null) {
    mouseState.hoverIndex = null;
  }
};

const onPointerUp = (event: any) => {
  event.target.onpointermove = null;
  event.target.releasePointerCapture(event.pointerId);
  columnState.dragIndex = null;
  columnState.pageXOnDragStart = null;
};

const onPointerDown = (
  event: {
    pointerId: number;
    clientX: number;
    target?: any;
    stopPropagation: () => void;
  },
  columnIndex: number,
) => {
  // stop the event from propagating up the DOM tree
  event.stopPropagation();

  // capture move events until the pointer is released
  if (event.target) {
    // event.pointerId is undefined
    event.target.setPointerCapture(event.pointerId);
  }
  columnState.dragIndex = columnIndex;
  columnState.pageXOnDragStart = event.clientX;
};

const onPointerMove = (event: { clientX: number }) => {
  let delta = event.clientX - (mouseState.currentX ?? event.clientX);
  if (typeof columnState.dragIndex === "number") {
    resizeColumns(delta);
  }
  mouseState.currentX = event.clientX;
};
</script>

<template>
  <thead>
    <tr>
      <th
        v-for="(header, ind) in columnHeaders"
        :key="ind"
        :style="{ width: `${columnSizes[ind]}px` }"
        class="column-header"
      >
        <span>{{ header }}</span>
        <div
          v-if="ind != columnHeaders.length - 1"
          :ref="`dragHandle-${ind}`"
          :class="[
            'drag-handle',
            {
              hover: mouseState.hoverIndex === ind,
              drag: columnState.dragIndex === ind,
            },
          ]"
          @pointerover="onPointerOver($event, ind)"
          @pointerleave="onPointerLeave"
          @pointerdown.passive="onPointerDown($event, ind)"
          @pointerup.passive="onPointerUp($event)"
          @pointermove="onPointerMove"
          @lostpointercapture="onLostPointerCapture"
        />
      </th>
    </tr>
  </thead>
</template>

<style lang="postcss" scoped>
thead {
  font-weight: 500;
  line-height: 15px;
  letter-spacing: 0;
  background-color: var(--knime-porcelain);
  text-align: left;
  position: sticky;
  top: 0;
  table-layout: fixed;
  width: calc(100% - 15px);

  & tr {
    display: flex;

    & th {
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
      text-align: left;
      display: flex;
      justify-content: space-between;

      & span {
        overflow: hidden;
        text-overflow: ellipsis;
        margin: 0 5px;
      }

      & .drag-handle {
        background-color: var(--knime-dove-gray);
        opacity: 0;
        width: 3px;
        cursor: col-resize;

        &.hover {
          opacity: 0.5;
          width: 5px;
        }

        &.drag {
          width: 2px;
          opacity: 1;
        }
      }
    }
  }
}
</style>
