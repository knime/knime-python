Zu dem `Execute failed: Cannot read DataCell with empty type information` Error.

Das passiert genau ab pandas 2.1.0 (wie das Problem mit den leeren Tables auch).

In der SDK ist der Error: `Cannot read the array length because "bytes" is null`. Warum ein andererer Error? In production ist `arrow.enable_null_check_for_get=false` und wir lesen einfach aus dem byte array und bekommen ein leeres byte array obwohl das validity bit auf "false" steht.

Das ist das Struct Dict Encoding Array sieht so aus:
- indices: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, ... 0, 0, 0, 0, 0, 0, 0, 0, 0, null]
- data: [null, null, null, null, null, null, null, null, null, null, ... null, null, null, null, null, null, null, null, null, null]] ALL MISSING!
- NOTE: Das data Array ist komplett null, aber sollte einen wert auf index 0 haben, weil darauf alle anderen indices verweisen.

Carsten hat schon angebracht, dass es irgendwie mit dem batching zu tun hat.

Ich hab mal die batch boundaries ausgegeben:
- Mit pandas 2.0.3: `256,512,768,998`
- Mit pandas 2.1.0: `590,998`

Ich weiß noch nicht genau was ich damit anfangen soll aber meine aktuelle Theorie ist, dass neu irgendwo ein re-batching stattfindet, dass unsere Struct-Dict-Encoded Arrays auseinander schneidet ohne, dass die indices angepasst werden.
Mir ist aber nicht klar, warum dann in den array von oben (der 2te batch mit einer Länge von 408) alle data values null sind anstatt non-null an den alten batch Grenzen.

---

## Root Cause Analysis

The bug is caused by **incompatible interaction between PyArrow's chunked array creation (changed in pandas 2.1.0) and KNIME's struct-dictionary encoding implementation**.

### The Core Problem

In `org.knime.python3.arrow/src/main/python/knime/_arrow/_types.py` (lines 130-148), when converting data to struct dict encoded arrays:

```python
def _get_arrow_storage_to_ext_fn(dtype):
    if is_dict_encoded_value_factory_type(dtype):
        key_gen = kasde.DictKeyGenerator()  # ← KEY GENERATOR CREATED ONCE PER COLUMN
        storage_fn = _get_arrow_storage_to_ext_fn(dtype.storage_type) or _identity

        def wrap_and_struct_dict_encode(a):
            unencoded_storage = storage_fn(a)
            encoded_storage = kasde.create_storage_for_struct_dict_encoded_array(
                unencoded_storage,
                key_gen,  # ← SAME KEY GENERATOR REUSED FOR ALL CHUNKS
                value_type=dtype.value_type,
                key_type=dtype.key_type,
            )
            return pa.ExtensionArray.from_storage(dtype, encoded_storage)

        return wrap_and_struct_dict_encode
```

**The key generator is created once per column** and stored in the closure of `wrap_and_struct_dict_encode`.

### What Changed in Pandas 2.1.0

Starting with pandas 2.1.0, `pa.Table.from_pandas()` creates chunked arrays with different batching behavior. When PyArrow processes a chunked array (via `_apply_to_array` in `_pandas.py` line 357):

```python
def _apply_to_array(array, func):
    if isinstance(array, pa.ChunkedArray):
        return pa.chunked_array([func(chunk) for chunk in array.chunks])
    else:
        return func(array)
```

**The same `wrap_and_struct_dict_encode` function (with the SAME `key_gen` instance) is called on each chunk.**

### The State Mismatch

In `_dictencoding.py` (lines 340-395), `create_storage_for_struct_dict_encoded_array` uses **local state** per array:

```python
def create_storage_for_struct_dict_encoded_array(array, key_generator, ...):
    entry_to_key = {}  # ← LOCAL DICTIONARY - RESETS FOR EACH CHUNK!
    
    for idx, v in enumerate(array):
        elif v in entry_to_key:
            # Already in this batch
            key = entry_to_key[v]
            keys.append(key)
            entry_indices.append(None)  # ← NO VALUE STORED
        else:
            # Not yet in this batch
            key = key_generator(v)  # ← GENERATOR CONTINUES COUNTING ACROSS CHUNKS
            entry_to_key[v] = key
            keys.append(key)
            entry_indices.append(idx)  # ← VALUE STORED ONLY FOR FIRST OCCURRENCE
```

### The Corruption Sequence

When processing the second chunk (rows 590-998):

1. **`entry_to_key = {}`** resets (empty dict) for the new chunk
2. All values appear "new" to this chunk, even if they existed in chunk 1
3. But **`key_generator` continues from where it left off** (e.g., next_key=42)
4. The second chunk generates keys continuing the sequence `[42, 43, 44, ...]`
5. **But the indices still reference old keys** (e.g., `0, 0, 0...`) from chunk 1
6. Since key `0` wasn't "first seen" in chunk 2, **chunk 2's data array has null at index 0**
7. When Java tries to decode, it looks for key `0` in chunk 2's dictionary, but finds only null!

### Concrete Example

```
Chunk 1 (rows 0-589):
  indices: [0, 0, 0, ...]          ← All reference key 0 (first unique value)
  data: ["actual_value", null, ...] ← Value stored at index 0 ✓
  key_gen.next_key: 42 after processing

Chunk 2 (rows 590-998):  
  entry_to_key: {}                 ← RESET! Key 0 no longer known
  indices: [0, 0, 0, ..., null]    ← Still reference key 0 from chunk 1!
  data: [null, null, null, ...]    ← Key 0 not in this chunk's dictionary! ✗
  key_gen.next_key: continues at 42
```

### Why the Symptoms

- **"Cannot read the array length because 'bytes' is null"** (SDK): When trying to read the value for key 0 in chunk 2, the data array entry is null
- **"Cannot read DataCell with empty type information"** (Production): Same issue but different error path due to `arrow.enable_null_check_for_get=false`
- **All data values are null in chunk 2**: Because none of the referenced keys (from chunk 1) were "first seen" in chunk 2

### Summary

The fundamental bug: **The key generator has global state across chunks, but the value dictionary (`entry_to_key`) has local state per chunk**. This creates a mismatch where indices reference keys that don't exist in the current chunk's dictionary.


---

# Manual Analysis by Marc and Benny


## Struct Dict Encoded Arrays are re-batched in pandas 2.1.0

In pandas 2.0.3 the Struct Dict Encoded Arrays stay chunked (as the orinial input into the node) when we do `kap.pandas_df_to_arrow(table.to_pandas())`:
```
# 1 for a single joined chunk of the primitive columns
# 3 for the 3 chunks of the Struct Dict Encoded Columns (as they were in the input table)
Chunks of the output columns: [1, 1, 1, 3, 1, 1, 3, 1, 1, 3, 1, 1, 3, 1, 1, 3, 1, 1, 3, 3, 3, 3, 3, 3]
```
Only the primitive columns are joined into a single chunk.

In pandas 2.1.0 also the Struct Dict Encoded Arrays are re-batched into 1 chunks:
```
Chunks of the output columns: [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
```
Additional note: The call to `pandas_df_to_arrow` took noticably longer in pandas 2.1.0 (several seconds) compared to pandas 2.0.3 (instantaneous).

## Table.to_batches experiments

- If we force a single batch only, no chunking happens and the output is valid in both pandas versions.
- If we force 20 batches, in pandas 2.0.3 the Struct Dict Encoded Arrays is chunked and the bug also happens for pandas 2.0.3.
- Table.to_batches did split at the existing batch boundaries of the already chunked columns and did not re-batch to the given number of `max_chunksize`.

## Open questions:
- What are the other columns that have 3 chunks in pandas 2.0.3? We do not have this many Struct Dict Encoded Columns in the test table.
- When does the re-batching happen in pandas 2.1.0? Why? Can we avoid it?
- We probably need to handle Struct Dict Encoded Arrays being split into ChunkedArrays. How can we do that?
- TODO: Integers can change to float if there are NaNs and there is no sentinel. This should be fixable now with a new pandas type.
