# Synapse's own answers for validate_builder and validate_new, as test
# vectors for validate.go (TestValidatorAgainstSynapse). Read-only: it builds
# events in memory and runs the validator on them. Regenerate with
#
#   docker exec -i synapse-matrix-synapse-1 python - \
#     < internal/creator/testdata/oracle_validator.py \
#     > internal/creator/testdata/oracle_validator.json
#
# Each case is [type, state_key or null, content as raw JSON]; the content is
# decoded with json.loads, as parse_json_object_from_request decodes a body.
import json, sys
from types import SimpleNamespace
from synapse.api.errors import SynapseError
from synapse.api.room_versions import KNOWN_ROOM_VERSIONS
from synapse.events import make_event_from_dict
from synapse.events.validator import EventValidator

ROOM = "!r:example.org"
SENDER = "@u:example.org"
CFG = SimpleNamespace(server=SimpleNamespace(server_name="example.org"))

M, T, N, TS, PL, RET, AL, ACL = ("m.room.message", "m.room.topic", "m.room.name", "m.room.tombstone",
    "m.room.power_levels", "m.room.retention", "m.room.aliases", "m.room.server_acl")
cases = [
    [M, None, '{"msgtype":"m.text","body":"x"}'],
    [M, None, '{"msgtype":"m.text"}'],
    [M, None, '{"body":"x"}'],
    [M, None, '{}'],
    [M, None, '{"body":1,"msgtype":"m.text"}'],
    [M, None, '{"body":null,"msgtype":"m.text"}'],
    [M, None, '{"body":"x","msgtype":2}'],
    [M, "", '{"body":"x","msgtype":"m.text"}'],
    [M, None, '{"body":"x","msgtype":"m.text","n":1.5}'],
    [M, None, '{"body":"x","msgtype":"m.text","n":9007199254740992}'],
    [M, None, '{"body":"x","msgtype":"m.text","n":-9007199254740992}'],
    [M, None, '{"body":"x","msgtype":"m.text","n":9007199254740991}'],
    [M, None, '{"body":"x","msgtype":"m.text","a":[1,{"b":2.0}]}'],
    [M, None, '{"body":"x","msgtype":"m.text","a":1e5}'],
    [M, None, '{"body":"x","msgtype":"m.text","a":1E400}'],
    [M, None, '{"n":1.5,"body":"x"}'],
    [M, None, '{"body":"x","msgtype":"m.text","a":[9007199254740992,1.5]}'],
    [M, None, '{"body":"x","msgtype":"m.text","a":-0}'],
    [T, "", '{"topic":"x"}'],
    [T, "", '{}'],
    [T, "", '{"topic":5}'],
    [T, None, '{"topic":"x"}'],
    [T, None, '{}'],
    [N, "", '{"name":"x"}'],
    [N, "", '{"name":null}'],
    [N, None, '{"name":"x"}'],
    [TS, "", '{"replacement_room":"!other:example.org"}'],
    [TS, "", '{}'],
    [TS, "", '{"replacement_room":"!r:example.org"}'],
    [TS, "", '{"replacement_room":5}'],
    [TS, "", '{"replacement_room":null}'],
    [TS, None, '{"replacement_room":"!other:example.org"}'],
    [TS, None, '{"replacement_room":"!r:example.org"}'],
    [RET, "", '{"max_lifetime":1000}'],
    [RET, None, '{"max_lifetime":1000}'],
    [RET, "", '{"min_lifetime":"1"}'],
    [RET, "", '{"min_lifetime":true}'],
    [RET, "", '{"max_lifetime":false}'],
    [RET, "", '{"min_lifetime":null,"max_lifetime":5}'],
    [RET, "", '{"min_lifetime":10,"max_lifetime":5}'],
    [RET, "", '{"min_lifetime":"x","max_lifetime":"y"}'],
    [RET, "", '{"min_lifetime":5,"max_lifetime":5}'],
    [AL, "example.org", '{"aliases":["#a:example.org"]}'],
    [AL, "example.org", '{"aliases":["#' + 'a' * 300 + ':example.org"]}'],
    [AL, "example.org", '{"aliases":"' + 'a' * 300 + '"}'],
    [AL, "example.org", '{"aliases":{"' + 'a' * 300 + '":1}}'],
    [AL, "example.org", '{"aliases":null}'],
    [AL, "example.org", '{"aliases":5}'],
    [AL, "example.org", '{"aliases":[5]}'],
    [AL, "example.org", '{}'],
    [ACL, "", '{"allow":["*"]}'],
    [ACL, "", '{"allow":["other.org"]}'],
    [ACL, "", '{"deny":["example.org"],"allow":["*"]}'],
    [ACL, "", '{"allow":["*"],"allow_ip_literals":false}'],
    [ACL, "", '{}'],
    [PL, "", '{"ban":50,"users":{"@u:example.org":100}}'],
    [PL, "", '{"ban":"50"}'],
    [PL, "", '{"ban":"50","kick":"x"}'],
    [PL, "", '{"kick":"x","ban":true}'],
    [PL, "", '{"users":{"@a:x":null}}'],
    [PL, "", '{"users":{"@a:x":null},"ban":"1"}'],
    [PL, "", '{"users":{"@a:x":"1","@b:x":true}}'],
    [PL, "", '{"users":5}'],
    [PL, "", '{"users":[1]}'],
    [PL, "", '{"events":{"m.room.name":{"a":1}}}'],
    [PL, "", '{"ban":null}'],
    [PL, "", '{"ban":[1,"a"]}'],
    [PL, "", '{"ban":{"a":1,"b":[true,null]}}'],
    [PL, "", '{"ban":"it\'s"}'],
    [PL, "", '{"ban":"say \\"hi\\""}'],
    [PL, "", '{"ban":"it\'s \\"x\\""}'],
    [PL, "", '{"ban":"é"}'],
    [PL, "", '{"ban":"a\\nb"}'],
    [PL, "", '{"ban":"\\u0001"}'],
    [PL, "", '{"ban":"​"}'],
    [PL, "", '{"ban":" "}'],
    [PL, "", '{"ban":"a\\\\b"}'],
    [PL, "", '{"ban":false}'],
    [PL, "", '{"users_default":"0","users":{"@a:x":"x"}}'],
    [PL, "", '{"notifications":{"room":"x"},"events":{"a":"y"}}'],
    [PL, "", '{"ban":1.5}'],
    [PL, "", '{"ban":9007199254740992}'],
    [PL, "", '{"redact":{}}'],
    [PL, "", '{"ban":"\U0001F600"}'],
    [PL, "", '{"ban":"\\u007f"}'],
    [PL, "", '{"ban":-0,"kick":"-0"}'],
    [PL, "", '{"events":{},"users":{},"notifications":{"room":50}}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":{}}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":{"user_ids":"x"}}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":{"room":1}}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":{"user_ids":[1]}}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":{"user_ids":["a",2,null]}}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":null}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":[]}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":"x"}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":{"room":null,"user_ids":3}}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":{"room":true,"user_ids":[1]}}'],
    [M, None, '{"body":"x","msgtype":"m.text","m.mentions":{"room":1,"user_ids":[1]}}'],
    [T, "", '{"topic":"x","m.mentions":{"room":1}}'],
    [PL, "", '{"ban":"x","m.mentions":{"room":1}}'],
    ["pt.custom", None, '{"anything":[1,2,{"a":null}]}'],
    ["pt.custom", None, '{"d":' + "[" * 125 + "]" * 125 + '}'],
    ["pt.custom", None, '{"d":' + "[" * 126 + "]" * 126 + '}'],
    ["pt.custom", None, '{"a":' + '{"a":' * 125 + '1' + '}' * 125 + '}'],
    [M, None, '{"msgtype":"m.text","body":"x","d":' + "[" * 200 + "1.5" + "]" * 200 + '}'],
    [M, None, '{"msgtype":"m.text","body":"x","d":' + "[" * 200 + "1E400" + "]" * 200 + '}'],
    [M, None, '{"msgtype":"m.text","d":' + "[" * 200 + "]" * 200 + '}'],
    ["pt.custom", "k", '{}'],
]

class Builder:
    # What validate_builder reads of an EventBuilder.
    def __init__(self, typ, sk, content):
        self.room_version = KNOWN_ROOM_VERSIONS["11"]
        self.type, self.content, self.room_id, self.sender = typ, content, ROOM, SENDER
        if sk is not None:
            self.state_key = sk

    def is_state(self):
        return hasattr(self, "state_key")


def answer(f):
    try:
        f()
        return None
    except SynapseError as e:
        return [e.code, e.errcode, e.msg]
    except Exception:
        return [500, "M_UNKNOWN", "Internal server error"]


out = []
for typ, sk, raw in cases:
    content = json.loads(raw)
    # create_event: validate_builder on the builder, then build, then
    # validate_new on the event.
    res = answer(lambda: EventValidator().validate_builder(Builder(typ, sk, content)))
    if res is not None:
        out.append([typ, sk, raw, res])
        continue
    d = {"type": typ, "content": content, "room_id": ROOM, "sender": SENDER,
         "auth_events": [], "prev_events": [], "hashes": {"sha256": "x"}, "signatures": {},
         "depth": 1, "origin_server_ts": 1, "unsigned": {"age_ts": 1}}
    if sk is not None:
        d["state_key"] = sk
    ev = []
    res = answer(lambda: ev.append(make_event_from_dict(d, KNOWN_ROOM_VERSIONS["11"])))
    if res is None:
        res = answer(lambda: EventValidator().validate_new(ev[0], CFG))
    out.append([typ, sk, raw, res])
json.dump(out, sys.stdout, indent=1, ensure_ascii=False)
print()
