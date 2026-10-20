module.exports = function (RED) {
    "use strict";
    const node_lox_ws_api = require("node-lox-ws-api");
    const http = require('http');
    const encMethods = {
        0: 'Token-Enc',
        1: 'AES-256-CBC',
        2: 'Hash'
    };

    RED.httpAdmin.get('/loxone-miniserver/struct', function (req, res) {
        if (!req.query.id) {
            return res.json("");
        }

        let configNode = RED.nodes.getNode(req.query.id);
        let result = {
            state: 'error',
            msg: 'miniserver not connected',
            structure: {}
        };

        if (!configNode) {
            result.msg = 'Could not get config node!'
            res.json(result);
            return;
        }

        if (configNode.active !== true) {
            result.msg = 'connection disabled'
        }

        if (configNode && configNode.active && configNode.connected) {
            result = {
                state: 'ok',
                msg: 'got miniserver structure',
                structure: configNode.structureData
            };
        }

        res.json(result);
    });

    RED.httpAdmin.get('/loxone-miniserver/struct-changed', function (req, res) {
        let result = {
            state: 'error',
            msg: 'miniserver not connected',
            structure: {}
        };

        let username = req.query.username;
        let password = req.query.password;
        let configNode = RED.nodes.getNode(req.query.id);
        if (configNode) {
            if (!username) {
                username = configNode.credentials.username;
            }
            if (!password) {
                password = configNode.credentials.password;
            }

            if (configNode.active !== true) {
                result.msg = 'connection disabled'
            }
        }

        http.get({
                host: req.query.host,
                port: req.query.port,
                path: '/data/LoxAPP3.json',
                auth: username + ':' + password,
            }, function (http_res) {
                if (http_res.statusCode !== 200) {
                    http_res.resume();
                    res.json(result);
                    return;
                }

                let data = '';
                http_res.on('data', function (chunk) {
                    data += chunk;
                });
                http_res.on('end', function () {
                    try {
                        result = {
                            state: 'ok',
                            msg: 'got miniserver structure',
                            structure: prepareStructure(JSON.parse(data)),
                        };
                    } catch (e) {
                        result.msg = 'invalid structure file';
                    }
                    res.json(result);
                });
            }
        ).on('error', function (e) {
            res.json(result);
        });
    });

    function LoxoneMiniserver(config) {
        function _updateEvent(uuid, evt) {
            node.handleEvent(uuid, evt);
        }

        function _limitString(text, limit) {
            if (text.length <= limit) {
                return text;
            }
            return text.substr(0, limit) + '...(' + text.length + ')';
        }

        RED.nodes.createNode(this, config);

        let node = this;
        node.connected = false;
        node.authenticated = false;
        node.connection = null;
        node.structureData = null;
        node.active = true; //config.active;
        node._onlineNodes = [];
        node._keepaliveNodes = [];
        node._inputNodes = [];
        node._outputNodes = [];
        node._streamInNodes = [];
        node._streamAllNodes = [];
        node._webserviceNodes = [];
        node._webserviceNodeQueue = []; //only webservice nodes which are waiting for a return will be here
        node._otherEvents = [];
        node._lastOnlineState = null;

        // TODO
        //do nothing if miniserver connection is not active
        /*
        if (config.active !== true) {
            node.log('connection to ' + config.host + ':' + config.port + ' disabled');

            node.setConnectionStatusMsg("grey", "connection disabled", "dot");
            return;
        }*/

        let text_logger_limit = 100;

        node.encMethod = 'Token-Enc';
        if (encMethods.hasOwnProperty(config.enctype)) {
            node.encMethod = encMethods[config.enctype];
        }

        if (!node.credentials.username || !node.credentials.password) {
            node.error("missing credentials for " + config.host + ':' + config.port);
            return;
        }

        let client = new node_lox_ws_api(
            config.host + ':' + config.port,
            node.credentials.username,
            node.credentials.password,
            true,
            node.encMethod,
            config.keepalive
        );
        node._client = client;

        client.on('connect', function () {
            node.log('Miniserver connected (' + config.host + ':' + config.port + ') using ' + node.encMethod);
        });

        client.on('authorized', function () {
            node.authenticated = true;
            node.connection = client;
        });

        client.on('auth_failed', function (message) {
            node.error('Miniserver authentication failed: ' + (typeof message === 'string' ? message : JSON.stringify(message)));
            node.setConnectionStatusMsg("red", "authentication failed", "ring");
        });

        client.on('reconnect', function (delay) {
            let retry = ", retry in " + Math.round(delay / 1000) + "s";
            if (node._connectFailed) {
                node.setConnectionStatusMsg("red", "connection failed" + retry, "ring");
            } else {
                node.setConnectionStatusMsg("yellow", "connection closed" + retry, "ring");
            }
        });

        client.on('get_structure_file', function (data) {
            node.log("got structure file " + data.lastModified);
            node.structureData = prepareStructure(data);
            node._connectFailed = false;
            node.connected = true;
            node.setConnectionStatusMsg("green", "connected", "dot");
            node.sendOnlineState(true);
        });

        client.on('connect_failed', function (error, reason) {
            node._connectFailed = true;
            node.error('Miniserver connect failed: ' + reason);
            node.setConnectionStatusMsg("red", "connection failed", "ring");
        });

        client.on('connection_error', function (error, reason) {
            node.error('Miniserver connection error: ' + reason);
            node.setConnectionStatusMsg("red", "connection error", "ring");
        });

        client.on('close', function (info, reason) {
            node.log("connection closed: " + reason, info);
            node.connected = false;
            node.authenticated = false;
            node.connection = null;


            //node.log('active on client close' + node.active);

            /*if (node.active !== true) {
                client.abort();
                node.setConnectionStatusMsg("grey", "connection disabled", "ring");
            } else {
                node.setConnectionStatusMsg("yellow", "connection closed", "ring");
            }*/

            node.setConnectionStatusMsg("yellow", "connection closed", "ring");
            node.sendOnlineState(false);
        });

        client.on('send', function (message) {
            //node.log("sent message: " + message);
        });

        client.on('message_text', function (message) {
            switch (message.type) {
                case 'json':
                    node.log("received text message: " + _limitString(JSON.stringify(message.data), text_logger_limit));

                    break;
                case 'control':
                    for (let i in node._webserviceNodeQueue) {
                        const wsNode = node._webserviceNodeQueue[i];
                        const handler = wsNode.handler;
                        const nodeMsg = wsNode.msg;
                        const uri = wsNode.uri;
                        if (uri === 'j' + message.control) {
                            let msg = Object.assign(nodeMsg, {
                                'payload': message.value,
                                'topic': message.control,
                                'code': parseInt(message.code)
                            });

                            //and parse all values to msg.data
                            if (message.hasOwnProperty('data') && message.data.hasOwnProperty('LL')) {
                                let additionalData = JSON.parse(JSON.stringify(message.data.LL));
                                delete additionalData.Code;
                                delete additionalData.control;

                                if (Object.keys(additionalData).length) {
                                    msg.data = additionalData;
                                }
                            }

                            //add miniserver info
                            if (node.structureData) {
                                msg.msInfo = node.structureData.msInfo;
                                msg.lastModified = node.structureData.lastModified;
                            } else {
                                msg.msInfo = null;
                                msg.lastModified = null;

                                node.log("structure data not ready yet, webservice request processed but without msInfo and lastModified");
                            }

                            //send the data out of the requesting node
                            handler.send(msg);

                            //unregister node from queue
                            node.removeWebserviceNodeFromQueue(handler, uri);
                            break;
                        }
                    }

                    break;
                default:
                    node.log("received text message: " + _limitString(String(message.data), text_logger_limit));
            }
        });

        client.on('keepalive', function (time) {
            for (let i = 0; i < node._keepaliveNodes.length; i++) {
                node._keepaliveNodes[i].send({
                    topic: 'keepalive',
                    payload: time,
                    msInfo: node.structureData ? node.structureData.msInfo : null,
                    lastModified: node.structureData ? node.structureData.lastModified : null
                });
            }
        });

        client.on('message_header', function (header) {
            //node.log('received message header (' + header.next_state() + '):');
            //console.log(header);
        });

        client.on('message_event_table_values', function (messages) {
            //node.log('received value messages:' + messages.length);
        });

        client.on('message_event_table_text', function (messages) {
            //node.log('received text messages:' + messages.length);
        });

        client.on('update_event_value', _updateEvent);
        client.on('update_event_text', _updateEvent);
        client.on('update_event_daytimer', _updateEvent);
        client.on('update_event_weather', _updateEvent);

        client.connect();

        this.on('close', function (done) {
            //on (full-)deploys shut the client down, also while it is (re)connecting,
            //otherwise the old client keeps running next to the new one
            let finished = false;
            let finish = function () {
                if (!finished) {
                    finished = true;
                    clearTimeout(timer);
                    done();
                }
            };
            let timer = setTimeout(finish, 5000);
            client.once('abort', finish);
            client.abort();
        });
    }

    RED.nodes.registerType("loxone-miniserver", LoxoneMiniserver, {
        credentials: {
            username: {type: "text"},
            password: {type: "password"}
        }
    });

    LoxoneMiniserver.prototype.registerOnlineNode = function (handler) {
        this._onlineNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerKeepaliveNode = function (handler) {
        this._keepaliveNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerInputNode = function (handler) {
        this._inputNodes.push(handler);
        this._inputNodesByState = null;
    };

    LoxoneMiniserver.prototype.registerOutputNode = function (handler) {
        this._outputNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerWebserviceNode = function (handler) {
        this._webserviceNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerStreamInNode = function (handler) {
        this._streamInNodes.push(handler);
    };

    LoxoneMiniserver.prototype.registerStreamAllNode = function (handler) {
        this._streamAllNodes.push(handler);
    };

    LoxoneMiniserver.prototype.addWebserviceNodeToQueue = function (handler, msg, uri) {
        this._webserviceNodeQueue.push({
            'handler': handler,
            'msg': msg,
            'uri': uri
        });
    };

    function removeFromList(list, handler) {
        const idx = list.indexOf(handler);
        if (idx !== -1) {
            list.splice(idx, 1);
        }
    }

    LoxoneMiniserver.prototype.deregisterOnlineNode = function (handler) {
        removeFromList(this._onlineNodes, handler);
    };

    LoxoneMiniserver.prototype.deregisterKeepaliveNode = function (handler) {
        removeFromList(this._keepaliveNodes, handler);
    };

    LoxoneMiniserver.prototype.deregisterInputNode = function (handler) {
        removeFromList(this._inputNodes, handler);
        this._inputNodesByState = null;
    };

    LoxoneMiniserver.prototype.deregisterOutputNode = function (handler) {
        removeFromList(this._outputNodes, handler);
    };

    LoxoneMiniserver.prototype.deregisterStreamInNode = function (handler) {
        removeFromList(this._streamInNodes, handler);
    };

    LoxoneMiniserver.prototype.deregisterStreamAllNode = function (handler) {
        removeFromList(this._streamAllNodes, handler);
    };

    LoxoneMiniserver.prototype.deregisterWebserviceNode = function (handler) {
        removeFromList(this._webserviceNodes, handler);
    };

    LoxoneMiniserver.prototype.removeWebserviceNodeFromQueue = function (handler, uri) {
        const idx = this._webserviceNodeQueue.findIndex(function (item) {
            return item.handler === handler && item.uri === uri;
        });
        if (idx !== -1) {
            this._webserviceNodeQueue.splice(idx, 1);
        }
    };

    LoxoneMiniserver.prototype.sendOnlineState = function (online) {
        if (this._lastOnlineState === online) {
            return;
        }
        this._lastOnlineState = online;
        for (let i = 0; i < this._onlineNodes.length; i++) {
            this._onlineNodes[i].send({
                payload: online
            });
        }
    };

    LoxoneMiniserver.prototype.setConnectionStatusMsg = function (color, text, shape) {
        shape = shape || 'dot';
        let newState = function (item) {
            item.status({
                fill: color,
                shape: shape,
                text: text
            });
        };

        this._inputNodes.forEach(newState);
        this._outputNodes.forEach(newState);
        this._streamInNodes.forEach(newState);
        this._streamAllNodes.forEach(newState);
        this._webserviceNodes.forEach(newState);
        this._onlineNodes.forEach(newState);
    };

    LoxoneMiniserver.prototype.findControlByState = function (uuid) {
        const entry = this.structureData ? this.structureData.stateIndex.get(uuid) : undefined;
        return entry ? entry.control : null;
    };

    function parsePayload(event) {
        //value events are numbers, daytimer and weather events objects, only text events need parsing
        if (typeof event !== 'string') {
            return event;
        }
        try {
            return JSON.parse(event);
        } catch (err) {
            return event;
        }
    }

    LoxoneMiniserver.prototype.buildMsgObject = function (event, uuid, entry) {
        const controlStructure = entry.control;
        return {
            payload: parsePayload(event),
            topic: controlStructure.name || null,
            state: entry.stateName,
            room: entry.room,
            category: entry.category,
            details: controlStructure.details || null,
            type: controlStructure.type || null,
            isFavorite: controlStructure.isFavorite || null,
            isSecured: controlStructure.isSecured || null,
            uuid: uuid || null,
            uuidAction: controlStructure.uuidAction || null,
            msInfo: this.structureData.msInfo,
            lastModified: this.structureData.lastModified
        };
    };

    LoxoneMiniserver.prototype._getInputNodesByState = function () {
        if (!this._inputNodesByState) {
            const byState = new Map();
            this._inputNodes.forEach(function (inputNode) {
                if (!byState.has(inputNode.state)) {
                    byState.set(inputNode.state, []);
                }
                byState.get(inputNode.state).push(inputNode);
            });
            this._inputNodesByState = byState;
        }
        return this._inputNodesByState;
    };

    LoxoneMiniserver.prototype.handleEvent = function (uuid, event) {
        let i, curNode, curRoom, curCategory;
        const entry = this.structureData ? this.structureData.stateIndex.get(uuid) : undefined;

        //do we have a control for this uuid? could also be weather or global
        if (!entry) {
            return;
        }
        const controlStructure = entry.control;

        //publish event to control-in nodes
        const inputNodes = this._getInputNodesByState().get(uuid);
        if (inputNodes) {
            for (i = 0; i < inputNodes.length; i++) {
                inputNodes[i].send(this.buildMsgObject(event, uuid, entry));
            }
        }

        //publish event to stream-all node
        for (i = 0; i < this._streamAllNodes.length; i++) {
            curNode = this._streamAllNodes[i];
            try {
                curNode.send(this.buildMsgObject(event, uuid, entry));
            } catch (error) {
                //TODO: error handling
            }
        }

        //publish event to stream-in node
        for (i = 0; i < this._streamInNodes.length; i++) {
            curNode = this._streamInNodes[i];
            curRoom = (curNode.hasOwnProperty('room')) ? curNode.room : null;
            curCategory = (curNode.hasOwnProperty('category')) ? curNode.category : null;

            //neither category or room present/filled, continue
            if (!curRoom && !curCategory) {
                continue;
            }

            if (
                (!curRoom || curRoom === controlStructure.room) &&
                (!curCategory || curCategory === controlStructure.cat)
            ) {
                curNode.send(this.buildMsgObject(event, uuid, entry));
            }
        }
    };

    function prepareStructure(data) {
        let structure = {
            rooms: data.rooms || {},
            cats: data.cats || {},
            controls: {},
            msInfo: data.msInfo || {},
            lastModified: data.lastModified
        };

        for (let uuid in data.controls) {
            if (!data.controls.hasOwnProperty(uuid)) {
                continue;
            }

            structure.controls[uuid] = data.controls[uuid];
            if (data.controls[uuid].hasOwnProperty('subControls')) {
                for (let sub_uuid in data.controls[uuid].subControls) {
                    if (!data.controls[uuid].subControls.hasOwnProperty(sub_uuid)) {
                        continue;
                    }

                    structure.controls[sub_uuid] = data.controls[uuid].subControls[sub_uuid];
                    structure.controls[sub_uuid].parent = uuid;
                    structure.controls[sub_uuid].room = data.controls[uuid].room;
                    structure.controls[sub_uuid].cat = data.controls[uuid].cat;
                }
            }
        }

        //index of all state uuids, built once per structure file instead of searching on every event
        const stateIndex = new Map();
        for (let controlUuid in structure.controls) {
            if (!structure.controls.hasOwnProperty(controlUuid)) {
                continue;
            }
            const control = structure.controls[controlUuid];
            if (!control.states) {
                continue;
            }
            const room = (control.room && structure.rooms[control.room] && structure.rooms[control.room].name) || null;
            const category = (control.cat && structure.cats[control.cat] && structure.cats[control.cat].name) || null;

            for (let stateName in control.states) {
                if (!control.states.hasOwnProperty(stateName)) {
                    continue;
                }
                const stateValue = control.states[stateName];
                const uuids = Array.isArray(stateValue) ? stateValue : [stateValue];
                for (let i = 0; i < uuids.length; i++) {
                    //first match wins, like the former linear search
                    if (typeof uuids[i] !== 'string' || stateIndex.has(uuids[i])) {
                        continue;
                    }
                    stateIndex.set(uuids[i], {
                        control: control,
                        stateName: Array.isArray(stateValue) ? stateName + ':' + i : stateName,
                        room: room,
                        category: category
                    });
                }
            }
        }
        //not enumerable, so the editor endpoint does not serialize it
        Object.defineProperty(structure, 'stateIndex', {value: stateIndex});

        return structure;
    }

    function LoxoneControlInNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.state = config.state;
        node.control = config.control;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerInputNode(node);
            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterInputNode(node);
                }

                done();
            });
        }
    }

    RED.nodes.registerType("loxone-control-in", LoxoneControlInNode);

    function LoxoneControlOutNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.control = config.control;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {

            node.miniserver.registerOutputNode(node);

            this.on('input', function (msg) {
                if (node.miniserver.connected && node.miniserver.connection) {
                    node.miniserver.connection.send_control_command(node.control, msg.payload);
                } else {
                    node.log(node.id + ' was triggered but miniserver is not connected.');
                }
            });

            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterOutputNode(node);
                }
                done();
            });
        }

    }

    RED.nodes.registerType("loxone-control-out", LoxoneControlOutNode);

    function LoxoneWebServiceNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerWebserviceNode(node);

            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterWebserviceNode(node);
                }
                done();
            });

            this.on('input', function (msg) {
                let wantedURI = msg.uri || config.uri;
                if (!wantedURI.length) {
                    node.status({
                        fill: 'red',
                        shape: 'ring',
                        text: 'empty uri'
                    });
                    return null;
                }

                if (!wantedURI.match(/^jdev/)) {
                    node.status({
                        fill: 'red',
                        shape: 'ring',
                        text: 'invalid uri'
                    });
                    return null;
                }

                node.uri = wantedURI + ((config.appendpayload === true) ? msg.payload : '');

                //add node to the queue for waiting messages and send the URI
                if (node.miniserver.connected && node.miniserver.connection) {
                    node.miniserver.addWebserviceNodeToQueue(node, msg, node.uri);
                    node.miniserver.connection.send_command(node.uri);
                } else {
                    node.status({
                        fill: 'red',
                        shape: 'dot',
                        text: 'not connected'
                    });
                    node.log(node.id + " tried to call " + wantedURI + " but miniserver is not connected.");
                    return null;
                }
            });
        }
    }

    RED.nodes.registerType('loxone-webservice', LoxoneWebServiceNode);

    function LoxoneOnlineNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.state = config.state;
        node.control = config.control;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerOnlineNode(node);

            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterOnlineNode(node);
                }
                done();
            });
        }
    }

    RED.nodes.registerType('loxone-online', LoxoneOnlineNode);

    function LoxoneKeepaliveNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerKeepaliveNode(node);

            this.on('close', function (done) {
                if (node.miniserver) {
                    node.miniserver.deregisterKeepaliveNode(node);
                }
                done();
            });
        }
    }

    RED.nodes.registerType('loxone-keepalive', LoxoneKeepaliveNode);
    RED.nodes.registerType('loxone-stream-in', LoxoneStreamInNode);

    function LoxoneStreamInNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.category = config.category;
        node.room = config.room;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {

            node.miniserver.registerStreamInNode(node);

            this.on('close', function (done) {

                if (node.miniserver) {
                    node.miniserver.deregisterStreamInNode(node);
                }

                done();
            });
        }
    }

    RED.nodes.registerType('loxone-stream-all', LoxoneStreamAllNode);

    function LoxoneStreamAllNode(config) {
        RED.nodes.createNode(this, config);
        let node = this;

        node.category = config.category;
        node.room = config.room;
        node.miniserver = RED.nodes.getNode(config.miniserver);
        if (node.miniserver) {
            node.miniserver.registerStreamAllNode(node);

            this.on('close', function (done) {

                if (node.miniserver) {
                    node.miniserver.deregisterStreamAllNode(node);
                }

                done();
            });
        }
    }
};
