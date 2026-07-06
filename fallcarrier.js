// FallCarrier · sovereign mobile connectivity layer
// WebRTC mesh + LoRa (Web Serial → Meshtastic) + WebSocket relay
// MIT · AI-Native Solutions

const DB_NAME = 'fallcarrier';
const DB_VERSION = 1;
const STORE_QUEUE = 'queue';
const STORE_LOG = 'log';

function openDB(){
  return new Promise((res, rej) => {
    const r = indexedDB.open(DB_NAME, DB_VERSION);
    r.onupgradeneeded = e => {
      const db = e.target.result;
      if(!db.objectStoreNames.contains(STORE_QUEUE)) db.createObjectStore(STORE_QUEUE, {keyPath:'id'});
      if(!db.objectStoreNames.contains(STORE_LOG)) db.createObjectStore(STORE_LOG, {keyPath:'id'});
    };
    r.onsuccess = () => res(r.result);
    r.onerror = () => rej(r.error);
  });
}

async function idbPut(store, val){
  const db = await openDB();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    tx.objectStore(store).put(val);
    tx.oncomplete = () => res(val);
    tx.onerror = () => rej(tx.error);
  });
}

async function idbAll(store){
  const db = await openDB();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readonly');
    const r = tx.objectStore(store).getAll();
    r.onsuccess = () => res(r.result || []);
    r.onerror = () => rej(r.error);
  });
}

async function idbDel(store, key){
  const db = await openDB();
  return new Promise((res, rej) => {
    const tx = db.transaction(store, 'readwrite');
    tx.objectStore(store).delete(key);
    tx.oncomplete = () => res();
    tx.onerror = () => rej(tx.error);
  });
}

const uid = () => 'm_' + Math.random().toString(36).slice(2, 10) + Date.now().toString(36);

export class FallCarrier extends EventTarget {
  constructor({ fallid = null, falllink = null, transports = ['webrtc','lora','websocket-relay'] } = {}) {
    super();
    this.fallid = fallid;
    this.falllink = falllink;
    this.enabled = new Set(transports);
    this.myDid = null;
    // peers: did -> { transports:{webrtc:{latency,link},lora:{device,latency},relay:{url,latency}}, lastSeen, hop }
    this.peers = new Map();
    this.loraDevices = []; // {port, reader, writer, name, channel, rssi}
    this.relays = []; // {url, ws, status}
    this.queue = []; // pending messages
    this.log = []; // sent history
    this.retryTimer = null;
  }

  async connect(){
    // identity
    if(this.fallid && typeof this.fallid.did === 'function'){
      try { this.myDid = await this.fallid.did(); } catch(e){}
    }
    if(!this.myDid){
      this.myDid = 'did:fc:' + Math.random().toString(36).slice(2, 12);
    }
    // WebRTC transport
    if(this.enabled.has('webrtc') && this.falllink){
      try {
        if(typeof this.falllink.on === 'function'){
          this.falllink.on('peer', p => this._onPeer(p));
          this.falllink.on('message', m => this._onIncoming(m, 'webrtc'));
        }
        if(typeof this.falllink.start === 'function') await this.falllink.start();
      } catch(e){ this._emit('transport-error', {transport:'webrtc', error:String(e)}); }
    }
    // load persisted queue + log
    try {
      this.queue = await idbAll(STORE_QUEUE);
      this.log = (await idbAll(STORE_LOG)).slice(-200);
    } catch(e){}
    this._emit('ready', {did:this.myDid});
    this.retryTimer = setInterval(() => this._drainQueue(), 5000);
    return this.myDid;
  }

  _emit(type, detail){ this.dispatchEvent(new CustomEvent(type, {detail})); }

  _onPeer(p){
    // p = { did, latency? }
    const did = p.did || p.id || p;
    if(!did) return;
    const existing = this.peers.get(did) || { transports:{}, hop:1, lastSeen:0 };
    existing.transports.webrtc = { latency: p.latency || 40, link: p.link || null };
    existing.hop = 1;
    existing.lastSeen = Date.now();
    this.peers.set(did, existing);
    this._emit('peer', {did, transport:'webrtc'});
  }

  _onIncoming(m, transport){
    // relay/hop forwarding: if not for us, forward
    if(!m || !m.to) return;
    if(m.to !== this.myDid){
      if(m.ttl && m.ttl > 0){
        m.ttl -= 1;
        m.path = (m.path || []).concat([this.myDid]);
        this._forward(m).catch(()=>{});
      }
      return;
    }
    this._emit('message', {from:m.from, payload:m.payload, transport, id:m.id, path:m.path||[]});
  }

  async _forward(m){
    // try direct peer with best transport
    const target = this.peers.get(m.to);
    if(target){
      const path = this._pickTransport(target);
      if(path === 'webrtc' && this.falllink && this.falllink.send){
        return this.falllink.send(m.to, m);
      }
      if(path === 'lora' && this.loraDevices.length){
        return this._sendLora(m);
      }
      if(path === 'relay' && this.relays.length){
        return this._sendRelay(m);
      }
    }
    // broadcast fallback via any transport
    if(this.falllink && this.falllink.broadcast) this.falllink.broadcast(m);
    if(this.loraDevices.length) this._sendLora(m);
    if(this.relays.length) this._sendRelay(m);
  }

  _pickTransport(peer){
    const t = peer.transports || {};
    // prefer lowest latency: webrtc(40) < relay(120) < lora(2000)
    const opts = [];
    if(t.webrtc) opts.push(['webrtc', t.webrtc.latency||40]);
    if(t.relay) opts.push(['relay', t.relay.latency||120]);
    if(t.lora) opts.push(['lora', t.lora.latency||2000]);
    opts.sort((a,b)=>a[1]-b[1]);
    return opts.length ? opts[0][0] : null;
  }

  async send(toDid, payload, opts = {}){
    const msg = {
      id: uid(),
      from: this.myDid,
      to: toDid,
      payload,
      ts: Date.now(),
      ttl: opts.ttl || 6,
      retries: 0,
      path: [this.myDid]
    };
    const ok = await this._attempt(msg);
    if(!ok){
      await idbPut(STORE_QUEUE, msg);
      this.queue.push(msg);
      this._emit('queued', {id:msg.id, to:toDid});
    } else {
      const entry = {id:msg.id, to:toDid, ts:msg.ts, status:'sent', transport:msg._sentVia, path:msg.path};
      this.log.push(entry);
      await idbPut(STORE_LOG, entry);
      this._emit('sent', entry);
    }
    return msg.id;
  }

  async _attempt(msg){
    const target = this.peers.get(msg.to);
    if(target){
      const t = this._pickTransport(target);
      if(t === 'webrtc' && this.falllink && this.falllink.send){
        try { await this.falllink.send(msg.to, msg); msg._sentVia = 'webrtc'; return true; } catch(e){}
      }
      if(t === 'lora' && this.loraDevices.length){
        try { await this._sendLora(msg); msg._sentVia = 'lora'; return true; } catch(e){}
      }
      if(t === 'relay' && this.relays.length){
        try { await this._sendRelay(msg); msg._sentVia = 'relay'; return true; } catch(e){}
      }
    }
    // no known route: broadcast on all transports (may find path)
    let any = false;
    if(this.falllink && this.falllink.broadcast){
      try { this.falllink.broadcast(msg); any = true; msg._sentVia = 'webrtc-bcast'; } catch(e){}
    }
    if(this.relays.length){
      try { await this._sendRelay(msg); any = true; msg._sentVia = 'relay'; } catch(e){}
    }
    if(this.loraDevices.length){
      try { await this._sendLora(msg); any = true; msg._sentVia = 'lora'; } catch(e){}
    }
    return any;
  }

  async _drainQueue(){
    if(!this.queue.length) return;
    const still = [];
    for(const msg of this.queue){
      msg.retries += 1;
      const ok = await this._attempt(msg);
      if(ok){
        await idbDel(STORE_QUEUE, msg.id);
        const entry = {id:msg.id, to:msg.to, ts:msg.ts, status:'sent', transport:msg._sentVia, retries:msg.retries, path:msg.path};
        this.log.push(entry);
        await idbPut(STORE_LOG, entry);
        this._emit('sent', entry);
      } else if(msg.retries < 20){
        still.push(msg);
      } else {
        await idbDel(STORE_QUEUE, msg.id);
        this._emit('dropped', {id:msg.id});
      }
    }
    this.queue = still;
  }

  broadcast(payload){
    const msg = { id: uid(), from: this.myDid, to: '*', payload, ts: Date.now(), ttl: 3, path:[this.myDid] };
    if(this.falllink && this.falllink.broadcast) this.falllink.broadcast(msg);
    if(this.loraDevices.length) this._sendLora(msg).catch(()=>{});
    if(this.relays.length) this._sendRelay(msg).catch(()=>{});
    return msg.id;
  }

  getRoute(toDid){
    const peer = this.peers.get(toDid);
    if(!peer){
      return { reachable: false, hops: [], note: 'no known peer · would broadcast on all transports' };
    }
    const t = this._pickTransport(peer);
    return {
      reachable: true,
      hops: [{ from: this.myDid, to: toDid, transport: t, latency: (peer.transports[t]||{}).latency }],
      totalLatency: (peer.transports[t]||{}).latency || 0
    };
  }

  getPeers(){
    const out = [];
    for(const [did, p] of this.peers){
      const t = this._pickTransport(p);
      out.push({
        did,
        transport: t,
        latency: (p.transports[t]||{}).latency,
        hop: p.hop || 1,
        available: Object.keys(p.transports),
        lastSeen: p.lastSeen
      });
    }
    return out;
  }

  // --- LoRa via Web Serial (Meshtastic) ---
  async subscribeLoRa(port){
    if(!('serial' in navigator) && !port){
      throw new Error('Web Serial not supported in this browser');
    }
    if(!port){
      port = await navigator.serial.requestPort();
    }
    await port.open({ baudRate: 115200 });
    const info = port.getInfo();
    const dev = {
      port,
      name: `LoRa-${info.usbVendorId||'?'}:${info.usbProductId||'?'}`,
      channel: 'LongFast',
      rssi: null,
      reader: null,
      writer: null
    };
    dev.writer = port.writable.getWriter();
    dev.reader = port.readable.getReader();
    this.loraDevices.push(dev);
    this._emit('lora-paired', {name:dev.name, channel:dev.channel});
    this._readLora(dev).catch(e => this._emit('lora-error', {error:String(e)}));
    return dev;
  }

  async _readLora(dev){
    // Meshtastic protobuf framing: START1(0x94) START2(0xC3) LEN(2 bytes) PROTOBUF
    let buf = new Uint8Array(0);
    while(true){
      const {value, done} = await dev.reader.read();
      if(done) break;
      const merged = new Uint8Array(buf.length + value.length);
      merged.set(buf); merged.set(value, buf.length);
      buf = merged;
      // scan for frame start
      let i = 0;
      while(i < buf.length - 4){
        if(buf[i] === 0x94 && buf[i+1] === 0xC3){
          const len = (buf[i+2] << 8) | buf[i+3];
          if(buf.length >= i + 4 + len){
            const payload = buf.slice(i+4, i+4+len);
            // v1: emit raw · full protobuf decode is v2
            this._emit('lora-frame', {
              device: dev.name,
              bytes: payload.length,
              hex: Array.from(payload.slice(0,32)).map(b=>b.toString(16).padStart(2,'0')).join(' ')
            });
            // synthetic rssi wobble for UI
            dev.rssi = -60 + Math.floor(Math.random()*30) - 20;
            i += 4 + len;
          } else break;
        } else {
          i += 1;
        }
      }
      buf = buf.slice(i);
    }
  }

  async _sendLora(msg){
    if(!this.loraDevices.length) throw new Error('no LoRa device');
    const dev = this.loraDevices[0];
    // v1: wrap payload as Meshtastic-style frame · protobuf body is app-defined here
    const body = new TextEncoder().encode(JSON.stringify({
      to: msg.to, from: msg.from, id: msg.id, payload: msg.payload
    }));
    // truncate to 230 bytes (LoRa MTU-ish)
    const clipped = body.slice(0, 230);
    const frame = new Uint8Array(4 + clipped.length);
    frame[0] = 0x94; frame[1] = 0xC3;
    frame[2] = (clipped.length >> 8) & 0xff;
    frame[3] = clipped.length & 0xff;
    frame.set(clipped, 4);
    await dev.writer.write(frame);
    this._emit('lora-tx', {bytes: frame.length, to: msg.to});
  }

  // --- WebSocket relay ---
  connectRelay(url){
    const rel = { url, ws: null, status: 'connecting' };
    try {
      rel.ws = new WebSocket(url);
      rel.ws.onopen = () => {
        rel.status = 'open';
        rel.ws.send(JSON.stringify({type:'hello', did:this.myDid}));
        this._emit('relay-open', {url});
      };
      rel.ws.onclose = () => { rel.status = 'closed'; this._emit('relay-close', {url}); };
      rel.ws.onerror = () => { rel.status = 'error'; this._emit('relay-error', {url}); };
      rel.ws.onmessage = ev => {
        try {
          const m = JSON.parse(ev.data);
          if(m.type === 'peer'){
            const existing = this.peers.get(m.did) || { transports:{}, hop:2, lastSeen:0 };
            existing.transports.relay = { url, latency: 120 };
            existing.lastSeen = Date.now();
            this.peers.set(m.did, existing);
            this._emit('peer', {did:m.did, transport:'relay'});
          } else if(m.type === 'msg'){
            this._onIncoming(m.payload, 'relay');
          }
        } catch(e){}
      };
    } catch(e){
      rel.status = 'error';
    }
    this.relays.push(rel);
    return rel;
  }

  async _sendRelay(msg){
    const rel = this.relays.find(r => r.status === 'open');
    if(!rel) throw new Error('no open relay');
    rel.ws.send(JSON.stringify({type:'msg', payload:msg}));
  }

  transportStatus(){
    return {
      webrtc: {
        enabled: this.enabled.has('webrtc'),
        peers: [...this.peers.values()].filter(p => p.transports.webrtc).length
      },
      lora: {
        enabled: this.enabled.has('lora'),
        devices: this.loraDevices.length,
        supported: 'serial' in navigator
      },
      relay: {
        enabled: this.enabled.has('websocket-relay'),
        open: this.relays.filter(r => r.status === 'open').length,
        total: this.relays.length
      }
    };
  }

  removeRelay(url){
    const idx = this.relays.findIndex(r => r.url === url);
    if(idx >= 0){
      try { this.relays[idx].ws.close(); } catch(e){}
      this.relays.splice(idx, 1);
    }
  }

  destroy(){
    if(this.retryTimer) clearInterval(this.retryTimer);
    for(const dev of this.loraDevices){
      try { dev.reader.cancel(); dev.writer.releaseLock(); dev.port.close(); } catch(e){}
    }
    for(const r of this.relays){ try { r.ws.close(); } catch(e){} }
  }
}

export default FallCarrier;
