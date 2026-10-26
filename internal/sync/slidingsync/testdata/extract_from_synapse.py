#!/usr/bin/env python3
"""Extract Synapse's own test table for `_required_state_changes` into JSON.

Copying the reference implementation's tests rather than writing our own from a
reading of its source is the point: that function has twenty-odd branches whose
interactions are the whole difficulty, and tests we wrote would mostly encode
our reading of the code, which is exactly what needs checking.

Parses the Python with `ast` rather than importing it, so it needs no Synapse
dependencies. Re-run after a Synapse upgrade:

    python3 internal/slidingsync/testdata/extract_from_synapse.py \
        internal/slidingsync/testdata/required_state_changes.json

Only the table on `RequiredStateChangesTestCase.test_xxx` is extracted. The two
procedural tests beside it (the remembered-keys cap) are ported by hand in
requiredstate_test.go, because they generate their inputs rather than listing
them.
"""
import ast, json, sys

SRC = "/home/daedric/synapse/tests/handlers/test_sliding_sync.py"
tree = ast.parse(open(SRC).read())

STATE_VALUES = {"WILDCARD": "*", "LAZY": "$LAZY", "ME": "$ME"}
EVENT_TYPES = {"Member": "m.room.member", "Create": "m.room.create",
               "Name": "m.room.name", "Topic": "m.room.topic",
               "RoomEncryption": "m.room.encryption", "CanonicalAlias": "m.room.canonical_alias",
               "JoinRules": "m.room.join_rules", "RoomHistoryVisibility": "m.room.history_visibility",
               "Tombstone": "m.room.tombstone", "SpaceChild": "m.space.child"}

def lit(node):
    """Evaluate the restricted literal grammar the table uses."""
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Tuple):
        return tuple(lit(e) for e in node.elts)
    if isinstance(node, (ast.Set, ast.List)):
        return [lit(e) for e in node.elts]
    if isinstance(node, ast.Dict):
        return {lit(k): lit(v) for k, v in zip(node.keys, node.values)}
    if isinstance(node, ast.Attribute):
        # StateValues.WILDCARD etc.
        if isinstance(node.value, ast.Name):
            owner = node.value.id
            if owner == "StateValues":
                return STATE_VALUES[node.attr]
            if owner == "EventTypes":
                return EVENT_TYPES[node.attr]
            if owner == "Membership":
                return node.attr.lower()
        raise ValueError(ast.dump(node))
    if isinstance(node, ast.Call):
        return call(node)
    if isinstance(node, ast.Name):
        raise ValueError("name " + node.id)
    raise ValueError(ast.dump(node)[:200])

def call(node):
    f = node.func
    name = f.attr if isinstance(f, ast.Attribute) else f.id
    owner = f.value.id if isinstance(f, ast.Attribute) and isinstance(f.value, ast.Name) else None

    if owner == "StateFilter":
        if name == "none":
            return {"kind": "none"}
        if name == "all":
            return {"kind": "all"}
        if name == "from_types":
            entries = []
            for t, k in lit(node.args[0]):
                entries.append({"type": t, "state_key": k})
            return {"kind": "types", "entries": entries}
        raise ValueError("StateFilter." + name)

    if name == "frozenset" or name == "set":
        return lit(node.args[0]) if node.args else []

    if name == "_RequiredStateChangesReturn":
        out = {"changed": None, "added": {"kind": "none"},
               "extra_lazy": [], "invalidated_lazy": []}
        fields = ["changed", "added", "extra_lazy", "invalidated_lazy"]
        for i, a in enumerate(node.args):
            out[fields[i]] = lit(a)
        for kw in node.keywords:
            m = {"changed_required_state_map": "changed",
                 "added_state_filter": "added",
                 "extra_users_to_add_to_lazy_cache": "extra_lazy",
                 "lazy_members_invalidated": "invalidated_lazy"}[kw.arg]
            out[m] = lit(kw.value)
        return out

    if name == "RequiredStateChangesTestParameters":
        out = {}
        for kw in node.keywords:
            out[kw.arg] = lit(kw.value)
        return out

    raise ValueError("call " + name)

cases = []
for node in ast.walk(tree):
    if not (isinstance(node, ast.ClassDef) and node.name == "RequiredStateChangesTestCase"):
        continue
    for dec in node.body[1].decorator_list if False else []:
        pass
    # find the parameterized.expand([...]) decorator on the test method
    for item in node.body:
        if not isinstance(item, ast.FunctionDef) or item.name != "test_xxx":
            continue
        for dec in item.decorator_list:
            if not isinstance(dec, ast.Call):
                continue
            fn = dec.func
            if not (isinstance(fn, ast.Attribute) and fn.attr == "expand"):
                continue
            for entry in dec.args[0].elts:
                if not isinstance(entry, ast.Tuple) or len(entry.elts) != 3:
                    continue
                label = lit(entry.elts[0])
                desc = lit(entry.elts[1])
                params = lit(entry.elts[2])
                cases.append({"name": label, "description": desc.strip(), **params})

def norm_map(m):
    return {k: sorted(v) for k, v in (m or {}).items()}

out = []
for c in cases:
    out.append({
        "name": c["name"],
        "description": c["description"],
        "prev": norm_map(c.get("previous_required_state_map")),
        "request": norm_map(c.get("request_required_state_map")),
        "state_deltas": [{"type": k[0], "state_key": k[1]} for k in (c.get("state_deltas") or {})],
        "previously_returned_lazy": sorted(c.get("previously_returned_lazy_user_ids") or []),
        "request_lazy": sorted(c.get("request_lazy_load_user_ids") or []),
        "with_deltas": {
            "changed": norm_map(c["expected_with_state_deltas"]["changed"]) if c["expected_with_state_deltas"]["changed"] is not None else None,
            "added": c["expected_with_state_deltas"]["added"],
            "extra_lazy": sorted(c["expected_with_state_deltas"]["extra_lazy"] or []),
            "invalidated_lazy": sorted(c["expected_with_state_deltas"]["invalidated_lazy"] or []),
        },
        "without_deltas": {
            "changed": norm_map(c["expected_without_state_deltas"]["changed"]) if c["expected_without_state_deltas"]["changed"] is not None else None,
            "added": c["expected_without_state_deltas"]["added"],
            "extra_lazy": sorted(c["expected_without_state_deltas"]["extra_lazy"] or []),
            "invalidated_lazy": sorted(c["expected_without_state_deltas"]["invalidated_lazy"] or []),
        },
    })

json.dump(out, open(sys.argv[1], "w"), indent=1, sort_keys=False)
print("extracted", len(out), "cases")
