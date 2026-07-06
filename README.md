# FallCarrier

**Sovereign mobile connectivity.** Mesh-first. Cell-optional. Works when the towers don't.

FallCarrier is a browser-native networking library that routes messages between peers over three transports — whichever is up:

- **WebRTC mesh** — same-network peers find each other and talk directly. No cell tower, no ISP hop.
- **LoRa radio** — pair a Meshtastic device (T-Beam, LILYGO T3, Heltec) via Web Serial for long-range low-bandwidth mesh (kilometres, not metres).
- **WebSocket relay** — when nobody local has a path out, hop through an internet relay. Optional. Configurable.

Messages queue to IndexedDB when all paths are down and drain when any path opens. No cloud dependency. No account. No tracking.

## Why this exists

The cellular network is a rented commons. Festivals congest it, rural areas lack it, outages break it, and it can be shut off. FallCarrier is the transport layer for the AI-Native Solutions estate that assumes the tower might not be there — and still gets the message through.

## Quick start

```html
<script type="module">
import { FallCarrier } from 'https://sjgant80-hub.github.io/fallcarrier/fallcarrier.js';
import { FallLink } from 'https://sjgant80-hub.github.io/falllink/falllink.js';
import { FallID } from 'https://sjgant80-hub.github.io/fallid/fallid.js';

const fc = new FallCarrier({
  fallid: new FallID(),
  falllink: new FallLink(),
  transports: ['webrtc', 'lora', 'websocket-relay']
});

await fc.connect();

// send to a peer DID — routes via best available path
await fc.send('did:fc:atrium9k', { text: 'field check' });

// see how a message would travel
fc.getRoute('did:fc:atrium9k');
// → { reachable: true, hops: [{ from: 'you', to: '...', transport: 'webrtc', latency: 38 }] }

// broadcast to all locally-reachable peers
fc.broadcast({ ping: Date.now() });

// pair a LoRa radio (Meshtastic node over USB serial)
await fc.subscribeLoRa();

// add a WebSocket relay
fc.connectRelay('wss://relay.example.org');

fc.addEventListener('message', e => console.log('got', e.detail));
fc.addEventListener('sent', e => console.log('delivered', e.detail));
fc.addEventListener('queued', e => console.log('queued — no path yet', e.detail));
</script>
```

## Live dashboard

Open [https://sjgant80-hub.github.io/fallcarrier/](https://sjgant80-hub.github.io/fallcarrier/). It shows:

- Transport status for WebRTC / LoRa / Relay
- Force-graph of reachable peers with edge colour by transport
- Message composer with `trace route` button
- LoRa pairing button (Web Serial API)
- WebSocket relay configuration
- Message log with delivery status and routes taken

## API

### `new FallCarrier({ fallid, falllink, transports })`
Construct with identity source, mesh discovery source, and which transports to enable.

### `await fc.connect()`
Initialises available transports. Resolves to your DID.

### `await fc.send(toDid, payload, opts?)`
Routes via best available path. Retries + persists to IndexedDB if all paths down. Drains queue every 5s.

### `fc.getRoute(toDid)`
Returns `{ reachable, hops[], totalLatency }` — how the message would travel right now.

### `fc.broadcast(payload)`
Sends to every reachable peer on every enabled transport.

### `fc.getPeers()`
`[{ did, transport, latency, hop, available[], lastSeen }, ...]`

### `await fc.subscribeLoRa(port?)`
Prompts Web Serial device picker (or uses supplied port). Opens serial at 115200 baud. Reads Meshtastic-framed traffic. Emits `lora-frame` and `lora-paired`.

### `fc.connectRelay(url)`
Opens a WebSocket relay peer. Emits `relay-open` / `relay-close` / `peer`.

### Events
`ready`, `peer`, `message`, `sent`, `queued`, `dropped`, `lora-paired`, `lora-frame`, `lora-tx`, `relay-open`, `relay-close`, `transport-error`.

## Transport priority

Route selection picks lowest-latency path per peer:
1. **WebRTC** (~40ms) — direct data channel
2. **Relay** (~120ms) — WebSocket hop
3. **LoRa** (~2s) — radio hop

If no known peer for the target, message fans out on all enabled transports (broadcast + queue).

## LoRa scope · honest note

**v1 ships a working Web Serial code path** that:
- Opens a serial connection to a paired Meshtastic device at 115200 baud
- Detects Meshtastic frame headers (`0x94 0xC3` + length prefix)
- Emits raw frame payloads with hex preview
- Writes outbound frames wrapped in the same header + a JSON body

**v1 does NOT do full Meshtastic protobuf parsing** (channel decoding, encryption, mesh routing table, ACKs). It's a detect-and-frame layer — enough to prove hardware works and get bytes on air, but not a fully compliant Meshtastic node. Full protobuf handling is v2.

Requires:
- Chrome / Edge 89+ (Web Serial API)
- A physical LoRa device (T-Beam, LILYGO T3, Heltec V3, etc.) with Meshtastic firmware, plugged in via USB

## Sovereignty

- All state lives in IndexedDB. Nothing leaves your device by default.
- No accounts. No telemetry. No third-party JS.
- WebSocket relays are opt-in. You choose the URL. Remove any time.
- MIT licensed. Fork it.

## Estate

Part of [AI-Native Solutions](https://ai-nativesolutions.com) — sovereign SMB infrastructure.

## License

MIT
