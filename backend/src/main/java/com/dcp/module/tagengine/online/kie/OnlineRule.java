package com.dcp.module.tagengine.online.kie;

import com.fasterxml.jackson.databind.JsonNode;

/** 在线求值的最小资产模型(规则快照: 与后端 tag_rule.def_json / 发布快照 assets 对齐) */
public class OnlineRule {
    public long id;
    public String kind;          // rule | flow
    public String name;
    public int ver;
    public JsonNode def;         // rule: {mode: visual|table|drl, ...}; flow: {steps:[...]}

    public static OnlineRule from(long id, String kind, String name, int ver, JsonNode def) {
        OnlineRule r = new OnlineRule();
        r.id = id;
        r.kind = kind;
        r.name = name;
        r.ver = ver;
        r.def = def;
        return r;
    }
}
