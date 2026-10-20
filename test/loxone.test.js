const test = require('node:test');
const assert = require('node:assert');
const helper = require('node-red-node-test-helper');
const MockMiniserver = require('node-lox-ws-api/test/mock-miniserver.js');
const loxoneNodes = require('../loxone/loxone.js');

helper.init(require.resolve('node-red'));

const STATE_ACTIVE = '0f1e2d3c-4b5a-6978-8796a5b4c3d2e1f0';
const STATE_POS = '1f1e2d3c-4b5a-6978-8796a5b4c3d2e1f0';
const STATE_SUB = '2f1e2d3c-4b5a-6978-8796a5b4c3d2e1f0';
const ROOM = '10000000-0000-0000-0000000000000001';
const CAT = '20000000-0000-0000-0000000000000001';

const structure = {
    lastModified: '2026-01-01 00:00:00',
    msInfo: {serialNr: 'TEST'},
    rooms: {[ROOM]: {name: 'Kitchen'}},
    cats: {[CAT]: {name: 'Lights'}},
    controls: {
        'c1': {name: 'Ceiling', type: 'Switch', uuidAction: 'c1', room: ROOM, cat: CAT, states: {active: STATE_ACTIVE}},
        'c2': {
            name: 'Blinds', type: 'Jalousie', uuidAction: 'c2', room: 'missing-room', states: {position: [STATE_POS]},
            subControls: {'c2/sub': {name: 'Sub', type: 'Switch', uuidAction: 'c2/sub', states: {active: STATE_SUB}}}
        },
    },
};

function wait(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

async function waitFor(check, timeout) {
    const end = Date.now() + (timeout || 3000);
    while (!check()) {
        if (Date.now() > end) {
            throw new Error('timeout');
        }
        await wait(20);
    }
}

function flow(ms) {
    const [host, port] = ms.host.split(':');
    return [
        {id: 'ms', type: 'loxone-miniserver', host: host, port: port, enctype: 2, keepalive: 30000},
        {id: 'in', type: 'loxone-control-in', miniserver: 'ms', control: 'c1', state: STATE_ACTIVE, wires: [['out_in']]},
        {id: 'all', type: 'loxone-stream-all', miniserver: 'ms', wires: [['out_all']]},
        {id: 'online', type: 'loxone-online', miniserver: 'ms', wires: [['out_online']]},
        {id: 'ctl', type: 'loxone-control-out', miniserver: 'ms', control: 'c1'},
        {id: 'out_in', type: 'helper'},
        {id: 'out_all', type: 'helper'},
        {id: 'out_online', type: 'helper'},
    ];
}

const credentials = {ms: {username: 'admin', password: 'secret'}};

function collect(node) {
    const msgs = [];
    node.on('input', (msg) => msgs.push(msg));
    return msgs;
}

test.afterEach(async () => {
    await helper.unload();
});

test('routes events to control-in, stream-all and online nodes', async () => {
    const ms = await new MockMiniserver({structure}).listen();
    await helper.load(loxoneNodes, flow(ms), credentials);
    const inMsgs = collect(helper.getNode('out_in'));
    const allMsgs = collect(helper.getNode('out_all'));
    const online = collect(helper.getNode('out_online'));
    const miniserver = helper.getNode('ms');

    await waitFor(() => miniserver.connected);
    await wait(50);
    ms.sendValues([{uuid: STATE_ACTIVE, value: 1}, {uuid: STATE_POS, value: 0.5}, {uuid: STATE_SUB, value: 0}]);
    await waitFor(() => allMsgs.length === 3);

    assert.strictEqual(inMsgs.length, 1);
    assert.strictEqual(inMsgs[0].payload, 1);
    assert.strictEqual(inMsgs[0].topic, 'Ceiling');
    assert.strictEqual(inMsgs[0].state, 'active');
    assert.strictEqual(inMsgs[0].room, 'Kitchen');
    assert.strictEqual(inMsgs[0].category, 'Lights');
    assert.strictEqual(inMsgs[0].msInfo.serialNr, 'TEST');

    assert.strictEqual(allMsgs[1].state, 'position:0');
    assert.strictEqual(allMsgs[1].room, null, 'unknown room must not crash');
    assert.strictEqual(allMsgs[2].topic, 'Sub');
    assert.deepStrictEqual(online.map((m) => m.payload), [true]);

    helper.getNode('ctl').receive({payload: 'on'});
    await waitFor(() => ms.received.some((m) => m.startsWith('jdev/sps/ios/') && m.endsWith('/c1/on')));
    await ms.close();
});

test('a dropped connection sends offline once and reconnects', async () => {
    const ms = await new MockMiniserver({structure}).listen();
    await helper.load(loxoneNodes, flow(ms), credentials);
    const online = collect(helper.getNode('out_online'));
    const miniserver = helper.getNode('ms');
    await waitFor(() => miniserver.connected);

    ms.dropAll();
    await waitFor(() => !miniserver.connected);
    await waitFor(() => miniserver.connected, 5000);
    assert.deepStrictEqual(online.map((m) => m.payload), [true, false, true]);
    await ms.close();
});

test('unknown text messages do not crash node-red', async () => {
    const ms = await new MockMiniserver({structure}).listen();
    await helper.load(loxoneNodes, flow(ms), credentials);
    const miniserver = helper.getNode('ms');
    await waitFor(() => miniserver.connected);

    ms.sockets.forEach((ws) => {
        ms.sendText(ws, JSON.stringify({hello: 'world'}));
        ms.sendText(ws, 'plain text');
    });
    await wait(200);
    assert.strictEqual(miniserver.connected, true);
    await ms.close();
});

test('redeploy while the miniserver is offline leaves no client behind', async () => {
    const ms = await new MockMiniserver({structure}).listen();
    const port = ms.port;
    await ms.close();

    await helper.load(loxoneNodes, flow({host: '127.0.0.1:' + port}), credentials);
    await wait(300);
    await helper.unload();

    // the miniserver comes back: nobody may connect any more
    const back = new MockMiniserver({structure});
    await new Promise((resolve) => back.server.listen(port, '127.0.0.1', resolve));
    await wait(2500);
    assert.strictEqual(back.httpRequests, 0);
    await back.close();
});

test('keepalive nodes do not crash before the structure file arrived', async () => {
    const ms = await new MockMiniserver({structure}).listen();
    const [host, port] = ms.host.split(':');
    await helper.load(loxoneNodes, [
        {id: 'ms', type: 'loxone-miniserver', host: host, port: port, enctype: 2, keepalive: 30000},
        {id: 'ka', type: 'loxone-keepalive', miniserver: 'ms', wires: [['out']]},
        {id: 'out', type: 'helper'},
    ], credentials);
    const msgs = collect(helper.getNode('out'));
    const miniserver = helper.getNode('ms');
    miniserver.structureData = null;
    miniserver.connection = null;
    // emit directly like the library does
    const client = miniserver._client;
    assert.ok(client, 'client is exposed for tests');
    client.emit('keepalive', 12);
    await wait(20);
    assert.strictEqual(msgs.length, 1);
    assert.strictEqual(msgs[0].payload, 12);
    assert.strictEqual(msgs[0].msInfo, null);
    await ms.close();
});
