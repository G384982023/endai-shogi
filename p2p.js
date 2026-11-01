// claude.ai の db・user と同じ呼び方で使える通信部分。PeerJS で端末どうしを直接つなぐ。
// 対局を作った端末が盤面を持ち、相手はその端末へつなぐ。作った側がページを閉じると対局は止まる。
(() => {
  const PREFIX = "endai-shogi-v1-";
  const HOST_KEY = (code) => "endai-host-" + code;

  const store = {
    get(k) { try { return JSON.parse(localStorage.getItem(k)); } catch (_) { return null; } },
    set(k, v) { try { localStorage.setItem(k, JSON.stringify(v)); } catch (_) {} },
  };

  const uid = (() => {
    let v = store.get("endai-uid");
    if (!v) { v = crypto.randomUUID(); store.set("endai-uid", v); }
    return v;
  })();

  const rooms = {};
  function room(code) {
    if (!rooms[code]) {
      const hosted = store.get(HOST_KEY(code));
      rooms[code] = { code, data: hosted, isHost: !!hosted, peer: null, hosting: null, conns: [], conn: null, connecting: null, fail: null, listeners: new Set() };
    }
    return rooms[code];
  }

  const snap = (r) => ({ exists: !!r.data, data: () => r.data, metadata: { fromCache: false } });
  const notify = (r) => { for (const cb of r.listeners) cb(snap(r)); };

  // ---------- 盤面を持つ側 ----------
  function host(r, reclaim) {
    if (r.hosting) return r.hosting;
    r.hosting = new Promise((res, rej) => {
      let tries = 0;
      const start = () => {
        const p = new Peer(PREFIX + r.code);
        p.on("open", () => { r.peer = p; res(); });
        p.on("connection", (c) => {
          c.on("open", () => { r.conns.push(c); c.send({ t: "doc", data: r.data }); });
          c.on("data", (m) => { if (m && m.t === "update") applyHost(r, m.patch); });
          c.on("close", () => { r.conns = r.conns.filter((x) => x !== c); });
        });
        p.on("disconnected", () => { if (!p.destroyed) p.reconnect(); });
        p.on("error", (e) => {
          if (r.peer) return;
          p.destroy();
          // 開き直した直後は前の接続がまだ名前を握っていることがある
          if (reclaim && e.type === "unavailable-id" && ++tries < 10) setTimeout(start, 2500);
          else { r.hosting = null; rej(e); }
        });
      };
      start();
    });
    return r.hosting;
  }

  function applyHost(r, patch) {
    r.data = { ...r.data, ...patch };
    store.set(HOST_KEY(r.code), r.data);
    for (const c of r.conns) if (c.open) c.send({ t: "doc", data: r.data });
    notify(r);
  }

  // ---------- 相手側 ----------
  let guestPeer = null;
  let guestReady = null;
  function getGuestPeer() {
    if (guestReady) return guestReady;
    guestReady = new Promise((res, rej) => {
      const p = new Peer();
      p.on("open", () => { guestPeer = p; res(p); });
      p.on("disconnected", () => { if (!p.destroyed) p.reconnect(); });
      p.on("error", (e) => {
        if (e.type === "peer-unavailable") {
          const r = Object.values(rooms).find((x) => String(e.message).includes(PREFIX + x.code));
          if (r && r.fail) r.fail();
          return;
        }
        if (!guestPeer) { guestReady = null; p.destroy(); rej(e); }
      });
    });
    return guestReady;
  }

  // 相手の盤面が届いたら true、その対局が無ければ false
  function connect(r) {
    if (r.connecting) return r.connecting;
    r.connecting = getGuestPeer().then((p) => new Promise((res, rej) => {
      let settled = false;
      const settle = (fn, v) => { if (settled) return; settled = true; clearTimeout(timer); r.connecting = null; r.fail = null; fn(v); };
      const c = p.connect(PREFIX + r.code, { reliable: true });
      const timer = setTimeout(() => { c.close(); settle(rej, new Error("timeout")); }, 12000);
      r.fail = () => settle(res, false);
      c.on("data", (m) => {
        if (!m || m.t !== "doc") return;
        r.conn = c;
        r.data = m.data;
        notify(r);
        settle(res, true);
      });
      c.on("close", () => {
        if (r.conn !== c) return;
        r.conn = null;
        if (r.listeners.size) retry(r);
      });
    }));
    r.connecting.catch(() => { r.connecting = null; });
    return r.connecting;
  }

  function retry(r) {
    setTimeout(() => {
      if (r.conn || !r.listeners.size) return;
      connect(r).then((ok) => { if (!ok) retry(r); }, () => retry(r));
    }, 3000);
  }

  // ---------- claude.ai と同じ形の窓口 ----------
  function doc(path) {
    const code = path.split("/")[1];
    const r = room(code);
    return {
      async get() {
        if (r.data) return snap(r);
        await connect(r);
        return snap(r);
      },
      async set(d) {
        r.data = d; r.isHost = true;
        await host(r, false);
        store.set(HOST_KEY(code), d);
        notify(r);
      },
      async update(patch) {
        if (r.isHost) { applyHost(r, patch); return; }
        if (!r.conn || !r.conn.open) throw new Error("not connected");
        r.conn.send({ t: "update", patch });
      },
      onSnapshot(cb) {
        r.listeners.add(cb);
        if (r.data) cb(snap(r));
        if (r.isHost) host(r, true).catch(() => {});
        else if (!r.conn) connect(r).then((ok) => { if (!ok) retry(r); }, () => retry(r));
        return () => r.listeners.delete(cb);
      },
      async acquire() { return { acquired: true }; },
    };
  }

  const db = { doc };
  const user = {
    async id() { return uid; },
    async can() { return true; },
    async profiles() { return {}; },
  };

  window.claude = { use: async (name) => (name === "db" ? db : name === "user" ? user : null) };
})();
