////// ----------------------- Connection "class" ---------------------- ////////////
// Patched fork: github.com/cavernerg/ioBroker.vis, branch "reactions" (base v1.5.6).
// Browsers cache this file for up to staticAssetCacheMaxAge (1h here), so a device may
// still be running the previous version. Type visConnFork in the console to find out
// which one it has.
var visConnFork = 'reactions/2026-09-22';
/* jshint browser: true */
/* global document */
/* global console */
/* global session */
/* global window */
/* global location */
/* global setTimeout */
/* global clearTimeout */
/* global io */
/* global $ */
/* global socketNamespace */
/* global socketUrl */
/* global socketSession */
/* jshint -W097 */
/* jshint strict: false */

'use strict';

window.getStoredObjects = function (name) {
    let objects = window.localStorage.getItem(name || 'objects');
    if (objects) {
        try {
            return JSON.parse(objects);
        } catch (e) {
            return null;
        }
    } else {
        return null;
    }
}

// The idea of servConn is to use this class later in every addon.
// The addon just must say, what must be loaded (values, objects, indexes) and
// the class loads it for addon. Authentication will be done automatically, so addon does not care about it.
// It will be .js file with localData and servConn

var servConn = {
    _socket:            null,
    _onConnChange:      null,
    _onUpdate:          null,
    _isConnected:       false,
    _disconnectedSince: null,
    _connCallbacks:     {
        onConnChange:   null,
        onUpdate:       null,
        onRefresh:      null,
        onAuth:         null,
        onCommand:      null,
        onError:        null
    },
    _authInfo:          null,
    _isAuthDone:        false,
    _isAuthRequired:    false,
    _authRunning:       false,
    _cmdQueue:          [],
    _connTimer:         null,
    _type:              'socket.io', // [SignalR | socket.io | local]
    _timeout:           0,           // 0 - use transport default timeout to detect disconnect
    _reconnectInterval: 10000,       // reconnect interval
    _reloadInterval:    30,          // if connection was absent longer than 30 seconds
    _reconnectionCount: 0,           // if we have many reconnections in a row, try to reload instead - workaround for https://github.com/ioBroker/ioBroker.vis/issues/332
    _cmdData:           null,
    _cmdInstance:       null,
    _isSecure:          false,
    _defaultMode:       0x644,
    _useStorage:        false,
    _objects:           null,        // used if _useStorage === true
    _enums:             null,        // used if _useStorage === true
    _autoSubscribe:     true,
    _subscribed:        [],          // every state pattern subscribed on this connection
    _getStatesQueue:    [],          // getStates calls waiting for the running one
    namespace:          'vis.0',

    _releaseGetStates: function () {
        // Setting the counter to 0 instead of decrementing also keeps it from going
        // negative when the disconnect handler has already reset it while an answer was
        // still on its way.
        this.gettingStates = 0;
        // Wake waiters until one of them actually takes the slot. One that bails out
        // early - no connection any more, or an empty list that needs no request - must
        // not strand the ones behind it; the polling this replaces could not deadlock
        // that way, so neither may the queue. Every round removes one entry, so this
        // always terminates.
        while (!this.gettingStates && this._getStatesQueue.length) {
            this._getStatesQueue.shift()();
        }
    },

    getType:          function () {
        return this._type;
    },
    getIsConnected:   function () {
        return this._isConnected;
    },
    getIsLoginRequired: function () {
        return this._isSecure;
    },
    getUser:          function () {
        return this._user;
    },
    setReloadTimeout: function (timeout){
        this._reloadInterval = parseInt(timeout, 10);
    },
    setReconnectInterval: function (interval){
        this._reconnectInterval = parseInt(interval, 10);
    },
    _checkConnection: function (func, _arguments) {
        if (!this._isConnected) {
            console.log('No connection!');
            return false;
        }

        if (this._queueCmdIfRequired(func, _arguments)) {
            return false;
        }

        //socket.io
        if (this._socket === null) {
            console.log('socket.io not initialized');
            return false;
        } else {
            return true;
        }
    },
    _monitor:         function () {
        if (this._timer) {
            return;
        }

        var ts = Date.now();
        if (this._reloadInterval && ts - this._lastTimer > this._reloadInterval * 1000) {
            // It seems, that PC was in a sleep => Reload page to request authentication anew
            this.reload();
        } else {
            this._lastTimer = ts;
        }

        var that = this;
        this._timer = setTimeout(function () {
            that._timer = null;
            that._monitor();
        }, 10000);
    },
    _onAuth:          function (objectsRequired, isSecure) {
        var that = this;

        this._isSecure = isSecure;

        if (this._isSecure) {
            that._lastTimer = Date.now();
            this._monitor();
        }

        this._autoSubscribe && this._socket.emit('subscribe', '*');
        objectsRequired && this._socket.emit('subscribeObjects', '*');

        // Subscriptions live on the server PER SOCKET, so a new socket starts without a
        // single one. Repeat everything that was subscribed before. This matters most
        // for widgets that subscribe from their own code (custom HTML dashboards):
        // vis re-subscribes only the IDs of its native widgets after a reconnect, so
        // without this the dashboards keep their values frozen and nobody notices -
        // until now only the forced page reload covered this up.
        if (this._subscribed.length) {
            this._socket.emit('subscribe', this._subscribed.slice());
        }

        if (this._isConnected === true) {
            // This seems to be a reconnect because we're already connected!
            // -> prevent firing onConnChange twice
            return;
        }

        this._isConnected = true;
        if (this._connCallbacks.onConnChange) {
            setTimeout(function () {
                that._socket.emit('authEnabled', function (auth, user) {
                    that._user = user;
                    that._connCallbacks.onConnChange(that._isConnected);
                    typeof app !== 'undefined' && app.onConnChange(that._isConnected);
                });
            }, 0);
        }
    },
    _isSelfReconnecting: function () {
        // The pure WebSocket client (@iobroker/ws) retries on its own, socket.io with
        // "reconnection: false" does not.
        return !!(this._socket && typeof this._socket._reconnect === 'function');
    },
    _isHandshakeRunning: function () {
        // connectingTimer is set while the client waits for the READY flag of a socket
        // it has just opened. A pending backoff timer (connectTimer) is explicitly NOT
        // checked here: pulling that attempt forward is exactly what we want.
        return !!(this._socket && this._socket.connectingTimer);
    },
    reconnect:        function (connOptions) {
        var that = this;
        if (++this._reconnectionCount >= 5) {
            this._reconnectionCount = 0; // reset counter
            // Escalate to a page reload only when there is no better way: a client that
            // reconnects on its own restores the session via the 'reconnect' event, and
            // while the browser knows it is offline a reload would only land on the
            // browser's error page - with the view gone for good.
            if (!this._isSelfReconnecting() && (typeof navigator === 'undefined' || navigator.onLine !== false)) {
                this.reload();
                return;
            }
        }
        // reconnect
        if ((!connOptions.mayReconnect || connOptions.mayReconnect()) && !this._connectInterval) {
            // A client that reconnects on its own has to stay the only owner of the
            // socket. Its connect() does not close the previous WebSocket, it just
            // replaces the reference - so a second driver here produces two live
            // sockets: the server authenticates and subscribes one of them while the
            // client reads from the other. The page is then "connected" but never
            // receives a single state update again. Only drive the retries for a client
            // that does not retry by itself; the countdown keeps running either way.
            var driveReconnect = !this._isSelfReconnecting();
            this._connectInterval = setInterval(function () {
                that._countDown = Math.floor(that._reconnectInterval / 1000);
                if (typeof $ !== 'undefined') {
                    $('.splash-screen-text').html(that._countDown + '...').css('color', 'red');
                }
                // Never interrupt a handshake that is already running: the pure WebSocket
                // client needs up to connectTimeout for it and a connect() call in between
                // throws the pending socket away. With a reconnectInterval shorter than
                // that timeout the connection could never be established at all.
                if (!driveReconnect || that._isHandshakeRunning()) {
                    return;
                }
                console.log('Trying connect...');
                that._socket.connect();
            }, this._reconnectInterval);

            this._countDown = Math.floor(this._reconnectInterval / 1000);
            if (typeof $ !== 'undefined') {
                $('.splash-screen-text').html(this._countDown + '...');
            }

            this._countInterval = setInterval(function () {
                that._countDown--;
                if (that._countDown < 0) {
                    // the next attempt may be deferred by a running handshake - do not
                    // count into the negative, that only looks broken
                    that._countDown = Math.floor(that._reconnectInterval / 1000);
                }
                if (typeof $ !== 'undefined') {
                    $('.splash-screen-text').html(that._countDown + '...');
                }
            }, 1000);
        }
    },
    _wakeUpCheck:     function () {
        var that = this;
        if (!this._socket || (typeof document !== 'undefined' && document.visibilityState === 'hidden')) {
            return;
        }
        if (!this._isConnected) {
            // Hurry the next attempt up instead of waiting for its timer - but only if
            // the client does not hold a socket right now. Its connect() does not close
            // the old one, so a second socket would be created, the first one orphaned,
            // and when the server drops the orphan the client tears down both. The
            // client keeps `socket` at null exactly while it waits between two attempts,
            // which is when hurrying up is safe.
            if (!this._isHandshakeRunning() && !this._socket.socket) {
                try {
                    this._socket.connect();
                } catch (e) {
                    // ignore
                }
            }
            return;
        }
        // We believe we are connected - verify it, the socket may be dead silently
        if (this._aliveCheckRunning) {
            return;
        }

        // Ask the cheap question first. The pure WebSocket client refreshes lastPong on
        // EVERY incoming frame, so its age is exactly the measure the client itself uses
        // to declare a connection dead - and after the realistic wake-up (phone locked
        // for minutes, timers frozen) it is far past the limit. Deciding from it saves
        // the whole probe, which is where almost all of the measured 4s went.
        var silence = this._socketSilence();
        if (silence !== null && silence > (this._socket.options && this._socket.options.pongTimeout || 6000)) {
            console.log(`Socket silent for ${Math.round(silence / 1000)}s at wake up => reconnect`);
            this._closeSocket();
            return;
        }

        this._aliveCheckRunning = true;
        var answered = false;
        try {
            this._socket.emit('getVersion', function () {
                answered = true;
            });
        } catch (e) {
            answered = false;
        }
        // A healthy connection answers in single-digit milliseconds; 1.5s is already
        // generous and is only reached in the grey zone the check above did not settle.
        setTimeout(function () {
            that._aliveCheckRunning = false;
            if (!answered && that._isConnected) {
                console.log('No answer from server after wake up => reconnect');
                that._closeSocket();
            }
        }, 1500);
    },
    _socketSilence:   function () {
        // Milliseconds since the last frame from the server, or null if the client does
        // not track it (socket.io).
        if (!this._socket || typeof this._socket.lastPong !== 'number' || !this._socket.lastPong) {
            return null;
        }
        return Date.now() - this._socket.lastPong;
    },
    _closeSocket:     function () {
        // close() of the ws client always schedules a reconnect on its own - there is no
        // flag that would switch that off, so this never leaves the page unconnected.
        try {
            this._socket.close ? this._socket.close() : this._socket.disconnect();
        } catch (e) {
            // ignore
        }
    },
    reload:           function () {
        if (window.location.host === 'iobroker.net' ||
            window.location.host === 'iobroker.biz' ||
            window.location.host === 'iobroker.pro') {
            window.location = '/';
        } else {
            window.location.reload();
        }
    },
    init:             function (connOptions, connCallbacks, objectsRequired, autoSubscribe) {
        var that = this; // support of old safari
        // init namespace
        if (typeof socketNamespace !== 'undefined') {
            this.namespace = socketNamespace;
        }

        connOptions = connOptions || {};
        if (!connOptions.name) {
            connOptions.name = this.namespace;
        }

        if (autoSubscribe !== undefined) {
            this._autoSubscribe = autoSubscribe;
        }

        // Correct "port only" url given from web adapter:
        if (window.socketUrl && window.socketUrl[0] === ':') {
            window.socketUrl = window.location.protocol + '//' + window.location.hostname + window.socketUrl;
        }

        // To start vis as local use one of:
        // - start vis from directory with name local, e.g. c:/blbla/local/ioBroker.vis/www/index.html
        // - do not create "_socket/info.js" file in "www" directory
        // - create "_socket/info.js" file with
        //   var socketUrl = "local"; var socketSession = ""; sysLang="en";
        //   in this case you can overwrite browser language settings
        if (document.URL.split('/local/')[1] || (typeof socketUrl === 'undefined' && !connOptions.connLink) || (typeof socketUrl !== 'undefined' && socketUrl === 'local')) {
            this._type = 'local';
        }

        if (typeof session !== 'undefined') {
            var user = session.get('user');
            if (user) {
                that._authInfo = {
                    user: user,
                    hash: session.get('hash'),
                    salt: session.get('salt')
                };
            }
        }

        this._connCallbacks = connCallbacks;

        var connLink = connOptions.connLink || window.localStorage.getItem('connLink');

        // Connection data from "/_socket/info.js"
        if (!connLink && typeof socketUrl !== 'undefined') {
            connLink = socketUrl;
        }
        if (!connOptions.socketSession && typeof socketSession !== 'undefined') {
            connOptions.socketSession = socketSession;
        }
        if (connOptions.socketForceWebSockets === undefined && typeof socketForceWebSockets !== 'undefined') {
            connOptions.socketForceWebSockets = socketForceWebSockets;
        }

        // if no remote data
        if (this._type === 'local') {
            // report connected state
            this._isConnected = true;
            this._connCallbacks.onConnChange && this._connCallbacks.onConnChange(this._isConnected);
            typeof app !== 'undefined' && app.onConnChange(this._isConnected);
        } else
        if (typeof io !== 'undefined') {
            connOptions.socketSession = connOptions.socketSession || 'nokey';

            var url;
            if (connLink) {
                if (typeof connLink !== 'undefined') {
                    if (connLink[0] === ':') {
                        connLink = location.protocol + '//' + location.hostname + connLink;
                    }
                }
                url = connLink;
            } else {
                url = location.protocol + '//' + location.host;
            }

            // remove port if via cloud
            if (url.match(/iobroker\.pro|iobroker\.net/)) {
                url = url.replace(/:\d+/, '');
            }

            this._socket = io.connect(url, {
                query:                          'key=' + connOptions.socketSession,
                'reconnection limit':           10000,
                'max reconnection attempts':    Infinity,
                reconnection:                   false,
                upgrade:                        !connOptions.socketForceWebSockets,
                rememberUpgrade:                connOptions.socketForceWebSockets,
                transports:                     connOptions.socketForceWebSockets ? ['websocket'] : undefined,
                // Options of the pure WebSocket client (@iobroker/ws). It ignores every
                // socket.io option above. Its default pongTimeout of 60s means that a
                // silently dropped connection (no FIN, e.g. Wi-Fi gone) is noticed only
                // after a minute, and the view shows stale values until then.
                //
                // pongTimeout counts "no message of ANY kind since", not "no pong for
                // this ping", so the tolerance against lost packets is
                // floor(pongTimeout / pingInterval) - 1 cycles: 2 here, the same as with
                // 5000/15000. What changes is how fast a dead line is noticed: measured
                // 12-17s with 5000/15000, 6s with these values, while re-connecting takes
                // 8ms and restoring the values 130ms. A false alarm used to cost a 28s
                // page reload, which is what justified the cautious setting; since this
                // fork it costs those 8ms, so there is nothing left to be cautious about.
                // The server pings after 5s of silence and closes after 15s (hard-coded
                // in @iobroker/ws-server), so pinging more often keeps it quiet.
                pongTimeout:                    parseInt(connOptions.pongTimeout, 10)     || 6000,
                pingInterval:                   parseInt(connOptions.pingInterval, 10)    || 2000,
                // Covers the WHOLE handshake up to the server's ready flag, including its
                // ACL lookup - not just the TCP connect. The client's default is 3000;
                // on a loaded host that is tight, and overrunning it throws the pending
                // socket away and starts over.
                connectTimeout:                 parseInt(connOptions.connectTimeout, 10)  || 6000,
                connectInterval:                parseInt(connOptions.connectInterval, 10) || 1000
            });

            // A tab that was in the background (phone locked, laptop closed) often comes
            // back with a socket that is dead without anybody having noticed yet. Check
            // it the moment the user returns instead of waiting for the next timer tick.
            if (!this._wakeUpBound) {
                this._wakeUpBound = true;
                var wakeUp = function () {
                    that._wakeUpCheck();
                };
                if (typeof document !== 'undefined' && document.addEventListener) {
                    document.addEventListener('visibilitychange', wakeUp, false);
                }
                if (typeof window !== 'undefined' && window.addEventListener) {
                    window.addEventListener('online', wakeUp, false);
                    window.addEventListener('pageshow', function (e) {
                        // pageshow fires on EVERY load, not only on a bfcache restore -
                        // acting on the normal load would open a second socket while the
                        // first one is still shaking hands
                        e && e.persisted && wakeUp();
                    }, false);
                }
            }

            var lastHandledSocket = null;

            var onConnect = function () {
                var raw = that._socket ? that._socket.socket : null; // the real WebSocket
                if (raw) {
                    if (raw === lastHandledSocket) {
                        // same connection, already restored
                        return;
                    }
                    lastHandledSocket = raw;
                    // This is a DIFFERENT connection than the one restored last: while we
                    // were busy the client may have thrown its socket away and opened a
                    // new one. Force the full restore for it, because _onAuth() bails out
                    // with "we are already connected" otherwise - and then this socket
                    // never sends 'subscribe'. The page looks connected, answers requests
                    // and stays mute for good: no state ever arrives again.
                    that._isConnected = false;
                } else if (that._lastConnectHandled && Date.now() - that._lastConnectHandled < 1000) {
                    // socket.io (no raw socket exposed) fires 'connect' AND 'reconnect'
                    // for the same connection - restore it only once
                    return;
                }
                that._lastConnectHandled = Date.now();
                that._reconnectionCount = 0; // reset counter

                // Disarm the watchdog of a PREVIOUS connection before arming a new one
                // below - the assignment further down only overwrites the reference, the
                // old timer keeps running and would reload the page although this
                // connection is healthy.
                if (that.waitConnect) {
                    clearTimeout(that.waitConnect);
                    that.waitConnect = null;
                }

                if (that._disconnectedSince) {
                    var offlineTime = Date.now() - that._disconnectedSince;
                    console.log('was offline for ' + (offlineTime / 1000) + 's');

                    // Reload the whole page only when authentication is in use, because
                    // then the session may have expired while we were offline. Without
                    // authentication there is nothing to renew and this handler restores
                    // views and all states anyway - without throwing the page away.
                    if (that._reloadInterval && offlineTime > that._reloadInterval * 1000 && !that.authError && that._isSecure) {
                        that.reload();
                    }

                    that._disconnectedSince = null;
                }

                if (that._connectInterval) {
                    clearInterval(that._connectInterval);
                    that._connectInterval = null;
                }
                if (that._countInterval) {
                    clearInterval(that._countInterval);
                    that._countInterval = null;
                }
                var elem = document.getElementById('server-disconnect');
                if (elem) {
                    elem.style.display = 'none';
                }

                that._socket.emit('name', connOptions.name);
                console.log(new Date().toISOString() + ' Connected => authenticate');

                setTimeout(function () {
                    var timeOut = 6000;
                    // If online give more time
                    if (window.location.href.indexOf('iobroker.') !== -1) {
                        timeOut = 12000;
                    }
                    that.waitConnect = setTimeout(function() {
                        console.error('No answer from server');
                        // Same reasoning as the offline-too-long reload above: only an
                        // authenticated session has something a reload could renew.
                        // Without authentication a reload would throw the page away for
                        // a server that is merely slow to answer; the ping/pong watchdog
                        // of the socket takes care of a connection that is really dead.
                        if (!that.authError && that._isSecure) {
                            that.reload();
                        }
                    }, timeOut);

                    that._socket.emit('authenticate', function (isOk, isSecure) {
                        if (that.waitConnect) {
                            clearTimeout(that.waitConnect);
                            that.waitConnect = null;
                        }

                        console.log(new Date().toISOString() + ' Authenticated: ' + isOk);

                        if (isOk) {
                            that._onAuth(objectsRequired, isSecure);
                        } else {
                            console.log('permissionError');
                        }
                    });
                }, 50);
            };

            this._socket.on('connect', onConnect);

            // The pure WebSocket client fires 'connect' only for the very first
            // connection and 'reconnect' for every later one. Without this line the VIS
            // session is never restored after a reconnect: the socket is up again, but
            // nothing re-subscribes or re-reads the states, so the only way out is the
            // emergency page reload above.
            this._socket.on('reconnect', onConnect);

            this._socket.on('reauthenticate', function (err) {
                if (that._connCallbacks.onConnChange) {
                    that._connCallbacks.onConnChange(false);
                    typeof app !== 'undefined' && !that.authError &&  app.onConnChange(false);
                }
                console.warn('reauthenticate');
                if (that.waitConnect) {
                    clearTimeout(that.waitConnect);
                    that.waitConnect = null;
                }

                if (connCallbacks.onAuthError) {
                    if (!that.authError) {
                        that.authError = true;
                        connCallbacks.onAuthError(err);
                    }
                } else {
                    that.reload();
                }
            });

            this._socket.on('connect_error', function () {
                if (typeof $ !== 'undefined') {
                    $('.splash-screen-text').css('color', '#002951');
                }

                that.reconnect(connOptions);
            });

            this._socket.on('disconnect', function () {
                that._disconnectedSince = Date.now();

                // Every request that was in flight is lost now - the client drops all
                // pending callbacks on close. Release the getStates guard here, because
                // a callback that never comes leaves it above zero and then EVERY later
                // getStates() blocks forever: the views keep their stale values and never
                // subscribe again. Until now the forced page reload hid this.
                that.gettingStates = 0;
                // Waiters would only run into the closed connection; drop them instead of
                // leaving them queued for a socket that no longer exists.
                that._getStatesQueue = [];

                // The 6s "no answer from server" watchdog of onConnect only ever gets
                // cleared by the answer itself. Leaving it armed across a disconnect means
                // it fires after the NEXT connect succeeded - and reloads the page, which
                // is exactly what this fork exists to avoid. It became reachable when
                // onConnect was bound to 'reconnect' as well, so onConnect now runs more
                // than once.
                if (that.waitConnect) {
                    clearTimeout(that.waitConnect);
                    that.waitConnect = null;
                }

                // called only once when connection lost (and it was here before)
                that._isConnected = false;
                if (that._connCallbacks.onConnChange) {
                    setTimeout(function () {
                        //show server disconnect layer only when socket is not reconnected in the 5s timeout
                        if (that._isConnected) {
                            return
                        }
                        var elem = document.getElementById('server-disconnect');
                        if (elem) {
                            elem.style.display = '';
                        }
                        that._connCallbacks.onConnChange(that._isConnected);
                        typeof app !== 'undefined' && app.onConnChange(that._isConnected);
                    }, 5000);
                } else {
                    var elem = document.getElementById('server-disconnect');
                    if (elem) {
                        elem.style.display = '';
                    }
                }

                // reconnect
                that.reconnect(connOptions);
            });

            this._socket.on('objectChange', function (id, obj) {
                // If cache used
                if (that._useStorage) {
                    let objects = that._objects || window.getStoredObjects();
                    if (objects) {
                        if (obj) {
                            objects[id] = obj;
                        } else if (objects[id]) {
                            delete objects[id];
                        }
                        window.localStorage.setItem('objects', JSON.stringify(objects));
                    }
                }

                that._connCallbacks.onObjectChange && that._connCallbacks.onObjectChange(id, obj);
            });

            this._socket.on('stateChange', function (id, state) {
                if (!id || state === null || typeof state !== 'object') {
                    return;
                }

                if (that._connCallbacks.onCommand && id === that.namespace + '.control.command') {
                    if (state.ack) {
                        return;
                    }

                    if (state.val &&
                        typeof state.val === 'string' &&
                        state.val[0] === '{' &&
                        state.val[state.val.length - 1] === '}') {
                        try {
                            state.val = JSON.parse(state.val);
                        } catch (e) {
                            console.log('Command seems to be an object, but cannot parse it: ' + state.val);
                        }
                    }

                    // if command is an object {instance: 'iii', command: 'cmd', data: 'ddd'}
                    if (state.val && state.val.instance) {
                        if (that._connCallbacks.onCommand(state.val.instance, state.val.command, state.val.data)) {
                            // clear state
                            that.setState(id, {val: '', ack: true});
                        }
                    } else if (that._connCallbacks.onCommand(that._cmdInstance, state.val, that._cmdData)) {
                        // clear state
                        that.setState(id, {val: '', ack: true});
                    }
                } else if (id === that.namespace + '.control.data') {
                    that._cmdData = state.val;
                } else if (id === that.namespace + '.control.instance') {
                    that._cmdInstance = state.val;
                } else if (that._connCallbacks.onUpdate) {
                    that._connCallbacks.onUpdate(id, state);
                }
            });

            this._socket.on('permissionError', function (err) {
                if (that._connCallbacks.onError) {
                    /* {
                     command:
                     type:
                     operation:
                     arg:
                     }*/
                    that._connCallbacks.onError(err);
                } else {
                    console.log('permissionError');
                }
            });

            this._socket.on('error', function (err) {
                if (err === 'Invalid password or user name') {
                    console.warn('reauthenticate');
                    if (that.waitConnect) {
                        clearTimeout(that.waitConnect);
                        that.waitConnect = null;
                    }

                    if (connCallbacks.onAuthError) {
                        if (!that.authError) {
                            that.authError = true;
                            connCallbacks.onAuthError(err);
                        }
                    } else {
                        that.reload();
                    }
                } else {
                    console.error('Socket error: ' + err);
                    if (typeof $ !== 'undefined') {
                        $('.splash-screen-text').css('color', '#002951');
                    }

                    that.reconnect(connOptions);
                }
            });
        }
    },
    logout:           function (callback) {
        if (!this._isConnected) {
            console.log('No connection!');
            return;
        }

        this._socket.emit('logout', callback);
    },
    getVersion:       function (callback) {
        if (!this._checkConnection('getVersion', arguments)) {
            return;
        }

        this._socket.emit('getVersion', function (error, version) {
            callback && callback(version || error);
        });
    },
    _rememberSubscribe: function (idOrArray, remove) {
        var list = Array.isArray(idOrArray) ? idOrArray : [idOrArray];
        for (var i = 0; i < list.length; i++) {
            if (!list[i]) {
                continue;
            }
            var pos = this._subscribed.indexOf(list[i]);
            if (remove) {
                pos !== -1 && this._subscribed.splice(pos, 1);
            } else if (pos === -1) {
                this._subscribed.push(list[i]);
            }
        }
    },
    subscribe:        function (idOrArray, callback) {
        // Remember it even if the command cannot be sent right now: the server keeps
        // subscriptions per socket, so everything has to be repeated on a new one.
        this._rememberSubscribe(idOrArray);

        if (!this._checkConnection('subscribe', arguments)) {
            return;
        }

        this._socket.emit('subscribe', idOrArray, callback);
    },
    unsubscribe:      function (idOrArray, callback) {
        this._rememberSubscribe(idOrArray, true);

        if (!this._checkConnection('unsubscribe', arguments)) {
            return;
        }

        this._socket.emit('unsubscribe', idOrArray, callback);
    },
    _checkAuth:       function (callback) {
        if (!this._isConnected) {
            console.log('No connection!');
            return;
        }
        //socket.io
        if (this._socket === null) {
            console.log('socket.io not initialized');
            return;
        }
        this._socket.emit('getVersion', function (error, version) {
            callback && callback(version || error);
        });
    },
    readFile:         function (filename, callback, isRemote) {
        if (!callback) {
            throw 'No callback set';
        }

        if (this._type === 'local') {
            const data = window.getStoredObjects(filename);
            callback(null, data || null);
        } else {
            if (!this._checkConnection('readFile', arguments)) {
                return;
            }

            if (!isRemote && typeof app !== 'undefined' && !app.settings.dontCache) {
                app.readLocalFile(filename.replace(/^\/vis\.0\//, ''), callback);
            } else {
                var adapter = this.namespace;
                if (filename[0] === '/') {
                    var p = filename.split('/');
                    adapter = p[1];
                    p.splice(0, 2);
                    filename = p.join('/');
                }

                this._socket.emit('readFile', adapter, filename, function (err, data, mimeType) {
                    setTimeout(function () {
                        callback(err, data, filename, mimeType);
                    }, 0);
                });
            }
        }
    },
    getMimeType:      function (ext) {
        if (ext.indexOf('.') !== -1) {
            ext = ext.toLowerCase().match(/\.[^.]+$/);
        }
        var _mimeType;
        if (ext === '.css') {
            _mimeType = 'text/css';
        } else if (ext === '.bmp') {
            _mimeType = 'image/bmp';
        } else if (ext === '.png') {
            _mimeType = 'image/png';
        } else if (ext === '.jpg') {
            _mimeType = 'image/jpeg';
        } else if (ext === '.jpeg') {
            _mimeType = 'image/jpeg';
        } else if (ext === '.gif') {
            _mimeType = 'image/gif';
        } else if (ext === '.tif') {
            _mimeType = 'image/tiff';
        } else if (ext === '.js') {
            _mimeType = 'application/javascript';
        } else if (ext === '.html') {
            _mimeType = 'text/html';
        } else if (ext === '.htm') {
            _mimeType = 'text/html';
        } else if (ext === '.json') {
            _mimeType = 'application/json';
        } else if (ext === '.xml') {
            _mimeType = 'text/xml';
        } else if (ext === '.svg') {
            _mimeType = 'image/svg+xml';
        } else if (ext === '.eot') {
            _mimeType = 'application/vnd.ms-fontobject';
        } else if (ext === '.ttf') {
            _mimeType = 'application/font-sfnt';
        } else if (ext === '.woff') {
            _mimeType = 'application/font-woff';
        } else if (ext === '.wav') {
            _mimeType = 'audio/wav';
        } else if (ext === '.mp3') {
            _mimeType = 'audio/mpeg3';
        } else {
            _mimeType = 'text/javascript';
        }
        return _mimeType;
    },
    readFile64:       function (filename, callback, isRemote) {
        var that = this;
        if (!callback) {
            throw 'No callback set';
        }

        if (!this._checkConnection('readFile64', arguments)) {
            return;
        }

        if (!isRemote && typeof app !== 'undefined' && !app.settings.dontCache) {
            app.readLocalFile(filename.replace(/^\/vis\.0\//, ''), function (err, data, mimeType) {
                setTimeout(function () {
                    if (data) {
                        callback(err, {mime: mimeType || that.getMimeType(filename), data: btoa(data)}, filename);
                    } else {
                        callback(err, filename);
                    }
                }, 0);
            });
        } else {
            var adapter = this.namespace;
            if (filename[0] === '/') {
                var p = filename.split('/');
                adapter = p[1];
                p.splice(0, 2);
                filename = p.join('/');
            }

            this._socket.emit('readFile64', adapter, filename, function (err, data, mimeType) {
                setTimeout(function () {
                    if (data) {
                        callback(err, {mime: mimeType || that.getMimeType(filename), data: data}, filename);
                    } else {
                        callback(err, {mime: mimeType || that.getMimeType(filename)}, filename);
                    }
                }, 0);
            });
        }
    },
    writeFile:        function (filename, data, mode, callback) {
        if (typeof mode === 'function') {
            callback = mode;
            mode = null;
        }
        if (this._type === 'local') {
            window.localStorage.setItem(filename, JSON.stringify(data));
            callback && callback();
        } else {
            if (!this._checkConnection('writeFile', arguments)) {
                return;
            }

            if (typeof data === 'object') {
                data = JSON.stringify(data, null, 2);
            }

            var parts = filename.split('/');
            var adapter = parts[1];
            parts.splice(0, 2);
            if (adapter === 'vis') {
                this._socket.emit('writeFile', adapter, parts.join('/'), data, mode ? {mode: this._defaultMode} : {}, callback);
            } else {
                this._socket.emit('writeFile', this.namespace, filename, data, mode ? {mode: this._defaultMode} : {}, callback);
            }
        }
    },
    // Write file base 64
    writeFile64:      function (filename, data, callback) {
        if (!this._checkConnection('writeFile64', arguments)) {
            return;
        }

        var parts = filename.split('/');
        var adapter = parts[1];
        parts.splice(0, 2);

        this._socket.emit('writeFile64', adapter, parts.join('/'), data, {mode: this._defaultMode}, callback);
    },
    readDir:          function (dirname, callback) {
        //socket.io
        if (this._socket === null) {
            console.log('socket.io not initialized');
            return;
        }
        dirname = dirname || '/';
        var parts = dirname.split('/');
        var adapter = parts[1];
        parts.splice(0, 2);

        this._socket.emit('readDir', adapter, parts.join('/'), {filter: true}, function (err, data) {
            callback && callback(err, data);
        });
    },
    mkdir:            function (dirname, callback) {
        var parts = dirname.split('/');
        var adapter = parts[1];
        parts.splice(0, 2);

        this._socket.emit('mkdir', adapter, parts.join('/'), function (err) {
            callback && callback(err);
        });
    },
    unlink:           function (name, callback) {
        var parts = name.split('/');
        var adapter = parts[1];
        parts.splice(0, 2);

        this._socket.emit('unlink', adapter, parts.join('/'), function (err) {
            callback && callback(err);
        });
    },
    renameFile:       function (oldname, newname, callback) {
        var parts1 = oldname.split('/');
        var adapter = parts1[1];
        parts1.splice(0, 2);
        var parts2 = newname.split('/');
        parts2.splice(0, 2);
        this._socket.emit('rename', adapter, parts1.join('/'), parts2.join('/'), function (err) {
            callback && callback(err);
        });
    },
    setState:         function (pointId, value, callback) {
        //socket.io
        if (this._socket === null) {
            //console.log('socket.io not initialized');
            return;
        }
        var that = this;
        // A command sent into a silently dropped connection is simply gone: the frame is
        // written, no error is raised, and the ws client neither checks the connection
        // here nor times out its callbacks (its own timeout is dead code in the shipped
        // build). Measured: 3s after a silent drop the page still believes it is
        // connected, the emit does not throw, and the callback never arrives. For the
        // operator that means a tap on a light switch does nothing and says nothing.
        // Insist on an acknowledgement so at least the caller finds out.
        var done = false;
        var timer = setTimeout(function () {
            if (done) {
                return;
            }
            done = true;
            console.warn(`setState ${pointId} was not acknowledged within 5s`);
            callback && callback('timeout');
        }, 5000);

        this._socket.emit('setState', pointId, value, function (err) {
            if (done) {
                return;
            }
            done = true;
            clearTimeout(timer);
            callback && callback(err);
        });
    },
    sendTo:         function (instance, command, payload, callback) {
        //socket.io
        if (this._socket === null) {
            //console.log('socket.io not initialized');
            return;
        }
        this._socket.emit('sendTo', instance, command, payload, callback);
    },
    // callback(err, data)
    getStates:        function (IDs, callback, _attempt) {
        if (typeof IDs === 'function') {
            callback = IDs;
            IDs = null;
        }
        var attempt = _attempt || 1;

        if (this._type === 'local') {
            return callback(null, []);
        }

        // An empty list can only ever be answered with {}. vis asks for exactly this
        // while booting - subscribing.active is still empty at that point, because the
        // views subscribe later from renderView. Do not spend a roundtrip on it.
        if (Array.isArray(IDs) && !IDs.length) {
            callback && setTimeout(function () {
                callback(null, {});
            }, 0);
            return;
        }

        if (!this._checkConnection('getStates', arguments)) {
            return;
        }
        var that = this;
        this.gettingStates = this.gettingStates || 0;
        if (this.gettingStates > 0) {
            // Only one request at a time - that part is original, it protects slow
            // devices. The original polled every 50 ms for the free slot, which costs
            // ~50 ms per queued call, and vis issues four of them while booting. Line
            // up and be woken instead.
            this._getStatesQueue.push(function () {
                that.getStates(IDs, callback, attempt);
            });
            return;
        }

        this.gettingStates++;

        // Safety net for an answer that never arrives (socket replaced or closed while
        // the request was in flight): the ws client drops pending callbacks without
        // calling them, and its own callback timeout is dead code in the shipped build.
        // Retry a few times, then fail loudly - retrying for ever would only hammer a
        // server that is merely slow, and the caller would never learn about it.
        var answered = false;
        var guard = setTimeout(function () {
            if (answered) {
                return;
            }
            answered = true;
            that._releaseGetStates();
            if (attempt >= 3) {
                console.error(`getStates got no answer in ${attempt} attempts => giving up`);
                callback && callback('timeout');
                return;
            }
            console.warn(`getStates got no answer within 10s => try again (${attempt + 1}/3)`);
            that.getStates(IDs, callback, attempt + 1);
        }, 10000);

        this._socket.emit('getStates', IDs, function (err, data) {
            if (answered) {
                return;
            }
            answered = true;
            clearTimeout(guard);
            that._releaseGetStates();
            if (err || !data) {
                callback && callback(err || 'Authentication required');
            } else if (callback) {
                callback(null, data);
            }
        });
    },
    _fillChildren:    function (objects) {
        var items = [];

        for (var id in objects) {
            if (!objects.hasOwnProperty(id)) {
                continue;
            }
            items.push(id);
        }
        items.sort();

        for (var i = 0; i < items.length; i++) {
            if (objects[items[i]].common) {
                var j = i + 1;
                var children = [];
                var len      = items[i].length + 1;
                var name     = items[i] + '.';
                while (j < items.length && items[j].substring(0, len) === name) {
                    children.push(items[j++]);
                }

                objects[items[i]].children = children;
            }
        }
    },
    getCharts: function (data, callback) {
        var that = this;
        // Check if chart view exists
        this._socket.emit('getObject', '_design/chart', function (err, obj) {
            if (obj && obj.views && obj.views.chart) {
                // Read all charts
                that._socket.emit('getObjectView', 'chart', 'chart', {startkey: '', endkey: '\u9999'}, function (err, res) {
                    if (err) {
                        callback(err);
                        return;
                    }
                    for (var i = 0; i < res.rows.length; i++) {
                        data[res.rows[i].value._id] = res.rows[i].value;
                    }
                    callback();
                });
            } else {
                callback();
            }
        });
    },
    // callback(err, data)
    getObjects:       function (useCache, callback) {
        if (typeof useCache === 'function') {
            callback = useCache;
            useCache = false;
        }
        // If cache used
        if (this._useStorage && useCache) {
            let objects = this._objects || window.getStoredObjects();
            if (objects) {
                return callback(null, objects);
            }
        }

        if (!this._checkConnection('getObjects', arguments)) {
            return;
        }
        var that = this;
        this._socket.emit('getObjects', function (err, data) {
            // Read all enums
            that._socket.emit('getObjectView', 'system', 'enum', {startkey: 'enum.', endkey: 'enum.\u9999'}, function (err, res) {
                if (err) {
                    return callback(err);
                }
                var enums = {};
                for (var i = 0; i < res.rows.length; i++) {
                    data[res.rows[i].id] = res.rows[i].value;
                    enums[res.rows[i].id] = res.rows[i].value;
                }

                // Read all adapters for images
                that._socket.emit('getObjectView', 'system', 'instance', {startkey: 'system.adapter.', endkey: 'system.adapter.\u9999'}, function (err, res) {
                    if (err) {
                        return callback(err);
                    }
                    for (var i = 0; i < res.rows.length; i++) {
                        data[res.rows[i].id] = res.rows[i].value;
                    }
                    // find out default file mode
                    if (data['system.adapter.' + that.namespace] &&
                        data['system.adapter.' + that.namespace].native &&
                        data['system.adapter.' + that.namespace].native.defaultFileMode) {
                        that._defaultMode = data['system.adapter.' + that.namespace].native.defaultFileMode;
                    }

                    // Read all charts
                    that.getCharts(data, function () {
                        // Read all channels for images
                        that._socket.emit('getObjectView', 'system', 'channel', {startkey: '', endkey: '\u9999'}, function (err, res) {
                            if (err) {
                                callback(err);
                                return;
                            }
                            for (var i = 0; i < res.rows.length; i++) {
                                data[res.rows[i].id] = res.rows[i].value;
                            }
                            // Read all devices for images
                            that._socket.emit('getObjectView', 'system', 'device', {
                                startkey: '',
                                endkey: '\u9999'
                            }, function (err, res) {
                                if (err) {
                                    return callback(err);
                                }
                                for (var i = 0; i < res.rows.length; i++) {
                                    data[res.rows[i].id] = res.rows[i].value;
                                }

                                if (that._useStorage) {
                                    that._fillChildren(data);
                                    that._objects = data;
                                    that._enums = enums;

                                    window.localStorage.setItem('objects', JSON.stringify(data));
                                    window.localStorage.setItem('enums', JSON.stringify(enums));
                                    window.localStorage.setItem('timeSync', Date.now().toString());
                                }

                                callback && callback(err, data);
                            });
                        });
                    });
                });
            });
        });
    },
    getChildren:      function (id, useCache, callback) {
        if (!this._checkConnection('getChildren', arguments)) {
            return;
        }

        if (typeof id === 'function') {
            callback = id;
            id = null;
            useCache = false;
        }
        if (typeof id === 'boolean') {
            callback = useCache;
            useCache = id;
            id = null;
        }
        if (typeof useCache === 'function') {
            callback = useCache;
            useCache = false;
        }

        if (!id) {
            return callback('getChildren: no id given');
        }

        var that = this;
        var data = [];

        if (this._useStorage && useCache) {
            const objects = window.getStoredObjects();
            if (objects && objects[id] && objects[id].children) {
                return callback(null, objects[id].children);
            }
        }

        // Read all devices
        that._socket.emit('getObjectView', 'system', 'device', {startkey: id + '.', endkey: id + '.\u9999'}, function (err, res) {
            if (err) {
                return callback(err);
            }
            for (var i = 0; i < res.rows.length; i++) {
                data[res.rows[i].id] = res.rows[i].value;
            }

            that._socket.emit('getObjectView', 'system', 'channel', {startkey: id + '.', endkey: id + '.\u9999'}, function (err, res) {
                if (err) {
                    return callback(err);
                }
                for (var i = 0; i < res.rows.length; i++) {
                    data[res.rows[i].id] = res.rows[i].value;
                }

                // Read all adapters for images
                that._socket.emit('getObjectView', 'system', 'state', {startkey: id + '.', endkey: id + '.\u9999'}, function (err, res) {
                    if (err) {
                        return callback(err);
                    }
                    for (var i = 0; i < res.rows.length; i++) {
                        data[res.rows[i].id] = res.rows[i].value;
                    }
                    var list = [];

                    var count = id.split('.').length;

                    // find direct children
                    for (var _id in data) {
                        if (data.hasOwnProperty(_id)) {
                            var parts = _id.split('.');
                            if (count + 1 === parts.length) {
                                list.push(_id);
                            }
                        }
                    }
                    list.sort();

                    if (that._useStorage) {
                        let objects = window.getStoredObjects() || {};

                        for (let id_ in data) {
                            if (data.hasOwnProperty(id_)) {
                                objects[id_] = data[id_];
                            }
                        }
                        if (objects[id] && objects[id].common) {
                            objects[id].children = list;
                        }
                        // Store for every element theirs children
                        var items = [];
                        for (var __id in data) {
                            if (data.hasOwnProperty(__id)) {
                                items.push(__id);
                            }
                        }
                        items.sort();

                        for (var k = 0; k < items.length; k++) {
                            if (objects[items[k]].common) {
                                var j = k + 1;
                                var children = [];
                                var len  = items[k].length + 1;
                                var name = items[k] + '.';
                                while (j < items.length && items[j].substring(0, len) === name) {
                                    children.push(items[j++]);
                                }

                                objects[items[k]].children = children;
                            }
                        }

                        window.localStorage.setItem('objects', JSON.stringify(objects));
                    }

                    callback && callback(err, list);
                });
            });
        });
    },
    getObject:        function (id, useCache, callback) {
        if (typeof id === 'function') {
            callback = id;
            id = null;
            useCache = false;
        }
        if (typeof id === 'boolean') {
            callback = useCache;
            useCache = id;
            id = null;
        }
        if (typeof useCache === 'function') {
            callback = useCache;
            useCache = false;
        }
        if (!id)
            return callback('no id given');

        // If cache used
        if (this._useStorage && useCache) {
            var objects = this._objects || window.getStoredObjects();
            if (objects && objects[id]) {
                return callback(null, objects[id]);
            }
        }

        var that = this;

        this._socket.emit('getObject', id, function (err, obj) {
            if (err) {
                callback(err);
                return;
            }
            if (that._useStorage) {
                var objects = window.getStoredObjects() || {};
                objects[id] = obj;
                window.localStorage.setItem('objects', JSON.stringify(objects));
            }
            return callback(null, obj);
        });
    },
    getGroups:        function (groupName, useCache, callback) {
        if (typeof groupName === 'function') {
            callback = groupName;
            groupName = null;
            useCache = false;
        }
        if (typeof groupName === 'boolean') {
            callback = useCache;
            useCache = groupName;
            groupName = null;
        }
        if (typeof useCache === 'function') {
            callback = useCache;
            useCache = false;
        }
        groupName = groupName || '';

        // If cache used
        if (this._useStorage && useCache) {
            const groups = this._groups || window.getStoredObjects('groups');
            if (groups) {
                return callback(null, groups);
            }
        }
        if (this._type === 'local') {
            return callback(null, []);
        } else {
            var that = this;
            // Read all enums
            this._socket.emit('getObjectView', 'system', 'group', {startkey: 'system.group.' + groupName, endkey: 'system.group.' + groupName + '\u9999'}, function (err, res) {
                if (err) {
                    return callback(err);
                }
                var groups = {};
                for (var i = 0; i < res.rows.length; i++) {
                    var obj = res.rows[i].value;
                    groups[obj._id] = obj;
                }
                if (that._useStorage) {
                    that._groups  = groups;

                    window.localStorage.setItem('groups', JSON.stringify(groups));
                }

                callback(null, groups);
            });
        }
    },
    getEnums:         function (enumName, useCache, callback) {
        if (typeof enumName === 'function') {
            callback = enumName;
            enumName = null;
            useCache = false;
        }
        if (typeof enumName === 'boolean') {
            callback = useCache;
            useCache = enumName;
            enumName = null;
        }
        if (typeof useCache === 'function') {
            callback = useCache;
            useCache = false;
        }

        // If cache used
        if (this._useStorage && useCache) {
            const enums = this._enums || window.getStoredObjects('enums');
            if (enums) {
                return callback(null, enums);
            }
        }

        if (this._type === 'local') {
            return callback(null, []);
        } else {

            enumName = enumName ? enumName + '.' : '';
            var that = this;
            // Read all enums
            this._socket.emit('getObjectView', 'system', 'enum', {startkey: 'enum.' + enumName, endkey: 'enum.' + enumName + '\u9999'}, function (err, res) {
                if (err) {
                    return callback(err);
                }
                var enums = {};
                for (var i = 0; i < res.rows.length; i++) {
                    var obj = res.rows[i].value;
                    enums[obj._id] = obj;
                }
                if (that._useStorage) {
                    window.localStorage.setItem('enums', JSON.stringify(enums));
                }
                callback(null, enums);
            });
        }
    },
    getLoggedUser:    function (callback) {
        this._socket.emit('authEnabled', callback);
    },
    // return time when the objects were synchronized
    getSyncTime:      function () {
        if (this._useStorage) {
            const timeSync = window.localStorage.getItem('timeSync');
            if (timeSync) {
                return new Date(timeSync);
            }
        }
        return null;
    },
    addObject:        function (objId, obj, callback) {
        if (!this._isConnected) {
            console.log('No connection!');
        } else
        //socket.io
        if (this._socket === null) {
            console.log('socket.io not initialized');
        }
    },
    delObject:        function (objId) {
        if (!this._checkConnection('delObject', arguments)) {
            return;
        }

        this._socket.emit('delObject', objId);
    },
    httpGet:          function (url, callback) {
        if (!this._isConnected) {
            return console.log('No connection!');
        }
        //socket.io
        if (this._socket === null) {
            return console.log('socket.io not initialized');
        }
        this._socket.emit('httpGet', url, function (data) {
            callback && callback(data);
        });
    },
    logError:         function (errorText) {
        console.log('Error: ' + errorText);
        if (!this._isConnected) {
            //console.log('No connection!');
            return;
        }
        //socket.io
        if (this._socket === null) {
            console.log('socket.io not initialized');
            return;
        }
        this._socket.emit('log', 'error', 'Addon vis ' + errorText);
    },
    _queueCmdIfRequired: function (func, args) {
        var that = this;
        if (!this._isAuthDone) {
            // Queue command
            this._cmdQueue.push({func: func, args: args});

            if (!this._authRunning) {
                this._authRunning = true;
                // Try to read version
                this._checkAuth(function (version) {
                    // If we have got version string, so there is no authentication, or we are authenticated
                    that._authRunning = false;
                    if (version) {
                        that._isAuthDone  = true;
                        // Repeat all stored requests
                        var __cmdQueue = that._cmdQueue;
                        // Trigger GC
                        that._cmdQueue = null;
                        that._cmdQueue = [];
                        for (var t = 0, len = __cmdQueue.length; t < len; t++) {
                            that[__cmdQueue[t].func].apply(that, __cmdQueue[t].args);
                        }
                    } else {
                        // Auth required
                        that._isAuthRequired = true;
                        // What for AuthRequest from server
                    }
                });
            }

            return true;
        } else {
            return false;
        }
    },
    authenticate:     function (user, password, salt) {
        this._authRunning = true;

        if (user !== undefined) {
            this._authInfo = {
                user: user,
                hash: password + salt,
                salt: salt
            };
        }

        if (!this._isConnected) {
            return console.log('No connection!');
        }

        if (!this._authInfo) {
            console.log("No credentials!");
        }
    },
    getConfig:        function (useCache, callback) {
        if (!this._checkConnection('getConfig', arguments)) {
            return;
        }

        if (typeof useCache === 'function') {
            callback = useCache;
            useCache = false;
        }
        if (this._useStorage && useCache) {
            var objects = window.getStoredObjects();
            if (objects && objects['system.config']) {
                return callback(null, objects['system.config'].common);
            }
        }
        var that = this;
        this._socket.emit('getObject', 'system.config', function (err, obj) {
            if (callback && obj && obj.common) {
                if (that._useStorage) {
                    let objects = window.getStoredObjects() || {};
                    objects['system.config'] = obj;
                    window.localStorage.setItem('objects', JSON.stringify(objects));
                }

                callback(null, obj.common);
            } else {
                callback('Cannot read language');
            }
        });
    },
    sendCommand:      function (instance, command, data, ack) {
        this.setState(this.namespace + '.control.instance', {val: instance || 'notdefined', ack: true});
        this.setState(this.namespace + '.control.data',     {val: data,    ack: true});
        this.setState(this.namespace + '.control.command',  {val: command, ack: ack === undefined ? true : ack});
    },
    _detectViews:     function (projectDir, callback) {
        this.readDir('/' + this.namespace + '/' + projectDir, function (err, dirs) {
            // find vis-views.json
            for (var f = 0; f < dirs.length; f++) {
                if (dirs[f].file === 'vis-views.json' && (!dirs[f].acl || dirs[f].acl.read)) {
                    return callback(err, {name: projectDir, readOnly: (dirs[f].acl && !dirs[f].acl.write), mode: dirs[f].acl ? dirs[f].acl.permissions : 0});
                }
            }
            callback(err);
        });
    },
    readProjects:     function (callback) {
        var that = this;
        this.readDir('/' + this.namespace, function (err, dirs) {
            var result = [];
            var count = 0;
            for (var d = 0; d < dirs.length; d++) {
                if (dirs[d].isDir) {
                    count++;
                    that._detectViews(dirs[d].file, function (subErr, project) {
                        project && result.push(project);
                        err = err || subErr;
                        !--count && callback(err, result);
                    });
                }
            }
        });
    },
    chmodProject:     function (projectDir, mode, callback) {
        //socket.io
        if (this._socket === null) {
            return console.log('socket.io not initialized');
        }
        this._socket.emit('chmodFile', this.namespace, projectDir + '*', {mode: mode}, function (err, data) {
            callback && callback(err, data);
        });
    },
    clearCache:       function () {
        window.localStorage.clear();
    },
    getHistory:       function (id, options, callback) {
        if (!this._checkConnection('getHistory', arguments)) {
            return;
        }

        options = options || {};
        options.timeout = options.timeout || 2000;

        var timeout = setTimeout(function () {
            timeout = null;
            callback('timeout');
        }, options.timeout);

        this._socket.emit('getHistory', id, options, function (err, result) {
            if (timeout) {
                clearTimeout(timeout);
                timeout = null;
            }
            callback(err, result);
        });
    },
    getLiveHost:      function (cb) {
        var that = this;
        this._socket.emit('getObjectView', 'system', 'host', {startkey: 'system.host.', endkey: 'system.host.\u9999'}, function (err, res) {
            var _hosts = [];
            for (var h = 0; h < res.rows.length; h++) {
                _hosts.push(res.rows[h].id + '.alive');
            }
            if (!_hosts.length) {
                return cb('');
            }
            that.getStates(_hosts, function (err, states) {
                for (var h in states) {
                    if (states.hasOwnProperty(h) && (states[h].val === 'true' || states[h].val === true)) {
                        return cb(h.substring(0, h.length - '.alive'.length));
                    }
                }

                cb('');
            });
        });
    },
    readDirAsZip:     function (project, useConvert, callback) {
        if (!callback) {
            callback = useConvert;
            useConvert = undefined;
        }
        if (!this._isConnected) {
            return console.log('No connection!');
        }
        //socket.io
        if (this._socket === null) {
            return console.log('socket.io not initialized');
        }
        if (project.match(/\/$/)) {
            project = project.substring(0, project.length - 1);
        }

        var that = this;
        this.getLiveHost(function (host) {
            if (!host) {
                return window.alert('No active host found');
            }
            // to do find active host
            that._socket.emit('sendToHost', host, 'readDirAsZip', {
                id: that.namespace,
                name: project || 'main',
                options: {
                    settings: useConvert
                }
            }, function (data) {
                data.error && console.error(data.error);
                callback && callback(data.error, data.data);
            });

        });
    },
    writeDirAsZip:    function (project, base64, callback) {
        if (!this._isConnected) {
            return console.log('No connection!');
        }
        //socket.io
        if (this._socket === null) {
            return console.log('socket.io not initialized');
        }
        if (project.match(/\/$/)) {
            project = project.substring(0, project.length - 1);
        }
        var that = this;
        this.getLiveHost(function (host) {
            if (!host) {
                return window.alert('No active host found');
            }
            that._socket.emit('sendToHost', host, 'writeDirAsZip', {
                id:   that.namespace,
                name: project || 'main',
                data: base64
            }, function (data) {
                data.error && console.error(data.error);
                callback && callback(data.error);
            });
        });
    }
};
