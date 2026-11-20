using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Haiven.Sdk;

namespace Haiven.Sdk.Tests
{
    static class PeerConnectionsTests
    {
        public static void Run(Action<string, bool> check)
        {
            SignalingLimits(check);
            SignalingFailures(check);
            P2PSessionsTests.Loop.Run(async () =>
            {
                double now = 0;
                var wire = new Signaling();
                var made = new List<ScriptedPeerTransport>();
                var reports = new List<string>();
                using var peers = new HaivenPeerConnections(wire, _ =>
                {
                    var transport = new ScriptedPeerTransport { HoldDescription = true };
                    made.Add(transport);
                    return transport;
                }, () => now);
                peers.Connected += user => reports.Add("connected:" + user);
                peers.Disconnected += (user, reason) => reports.Add(reason);

                check("guest needs a host", !peers.Start(false));
                check("host starts without creating an offer", peers.Start(true) && made.Count == 0);
                wire.Deliver("guest", PeerSignal.Offer("first"));
                var first = made.Single();
                wire.Deliver("guest", PeerSignal.ForCandidate(new PeerIceCandidate("candidate:1", "0", 0)));
                check("candidate waits for SetRemoteDescription", first.Candidates.Count == 0);
                first.CompleteDescription();
                await Task.Yield();
                check("host applies only the signaled offer and then its candidate",
                    first.RemoteSdp == "first" && first.Candidates.Single().Candidate == "candidate:1");
                first.Open();
                first.Open();
                check("both channels report connected once", reports.Count(s => s == "connected:guest") == 1);
                now = 100;
                peers.Tick();
                check("an open peer does not time out", !first.Disposed);

                wire.Deliver("guest", PeerSignal.Offer("second"));
                var second = made.Last();
                check("new offer replaces and disposes the old attempt", first.Disposed && second != first);
                for (var i = 0; i < 64; i++)
                    wire.Deliver("guest", PeerSignal.ForCandidate(new PeerIceCandidate("candidate:" + i, "0", 0)));
                check("64 candidates fit while the description is pending", !second.Disposed);
                wire.Deliver("guest", PeerSignal.ForCandidate(new PeerIceCandidate("candidate:65", "0", 0)));
                check("65th candidate fails the attempt", second.Disposed && reports.Last() == "candidate_buffer_full");
                var sent = wire.Sent.Count;
                second.CompleteDescription();
                await Task.Yield();
                check("late answer after a failed attempt sends nothing", wire.Sent.Count == sent);

                wire.Deliver("guest", PeerSignal.Offer("third"));
                var third = made.Last();
                now += 20;
                peers.Tick();
                check("pending SetRemoteDescription times out", third.Disposed && reports.Last() == "connect_timeout");
                peers.Dispose();
                check("core can start again after stopping", peers.Start(false, "host"));
                var guest = made.Last();
                wire.Deliver("stranger", PeerSignal.Answer("forged"));
                check("guest ignores answers from anyone but the host", guest.RemoteSdp == null);
                guest.CompleteDescription();
                await Task.Yield();
                wire.Deliver("host", PeerSignal.Answer("answer"));
                await Task.Yield();
                guest.Open();
                check("guest accepts its host's answer", guest.RemoteSdp == "answer" && reports.Last() == "connected:host");
                peers.Dispose();
                check("stop closes the transport", guest.Disposed);
                sent = wire.Sent.Count;
                guest.EmitCandidate();
                wire.Deliver("guest", PeerSignal.Offer("after-stop"));
                check("stopped core has no signaling callbacks", wire.Sent.Count == sent && made.Last() == guest);
                wire.DiscoverHostOnStart = true;
                check("test signaling can discover its host during Start", peers.Start(false));
            });
        }

        private static void SignalingLimits(Action<string, bool> check)
        {
            var socket = new Socket();
            using var core = new HaivenSessionCore(socket, () => "token");
            using var channel = new HaivenSessionChannel(core, "conn-1");
            core.Opened();
            socket.Deliver("{\"type\":\"auth.success\",\"success\":true}");
            var signaling = new HaivenSessionSignaling(channel, "s-1", "host", null);
            var reasons = new List<string>();
            using var peers = new HaivenPeerConnections(signaling, _ => new ScriptedPeerTransport());
            peers.Disconnected += (_, reason) => reasons.Add(reason);
            peers.Start(true);

            void Deliver(int epoch, int seq, object data)
            {
                socket.Deliver(System.Text.Json.JsonSerializer.Serialize(new
                {
                    type = "p2p.signal.received",
                    payload = new
                    {
                        sessionId = "s-1", fromUserId = "guest", attemptId = "attempt-" + epoch, seq,
                        data = System.Text.Json.JsonSerializer.Serialize(data),
                    },
                }));
                peers.Tick();
            }

            for (var seq = 2; seq <= 66; seq++)
                Deliver(1, seq, new { kind = "candidate", participationId = "p-guest", epoch = 1, candidate = "candidate:1" });
            check("65 candidates behind a missing sequence fail the attempt", reasons.SequenceEqual(new[] { "candidate_buffer_full" }));
            check("candidate overflow removes the pending peer", !peers.ConnectedUsers().Any());
            reasons.Clear();
            for (var part = 1; part <= 16; part++)
                Deliver(2, part, new { kind = "offer", participationId = "p-guest", epoch = 2, part, parts = 16, sdp = new string('x', 2200) });
            check("incoming SDP is capped at 32 KiB before assembly", reasons.SequenceEqual(new[] { "sdp_too_large" }));
            check("oversized assembly leaves no pending acknowledgements", signaling.PendingAcknowledgements == 0);
        }

        private static void SignalingFailures(Action<string, bool> check)
        {
            foreach (var hosting in new[] { true, false })
            foreach (var connected in new[] { true, false })
            foreach (var refused in new[] { true, false })
            {
                double now = 0;
                var socket = new Socket();
                using var core = new HaivenSessionCore(socket, () => "token");
                using var channel = new HaivenSessionChannel(core, "conn-1");
                core.Opened();
                socket.Deliver("""{"type":"auth.success","success":true,"payload":{"connectionId":"conn-1","contract":{"surface":"gaming","surfaceMajor":1,"surfaceRevision":"0.0.0-preview","descriptionDigest":"sha256:0000000000000000000000000000000000000000000000000000000000000000","capabilities":[],"subprotocol":"haiven.gaming.v0"}}}""");
                var remote = hosting ? "guest" : "host";
                var signaling = new HaivenSessionSignaling(channel, "s-1", hosting ? "host" : "guest",
                    hosting ? null : "host", () => now, "p-guest");
                var transport = new ScriptedPeerTransport();
                var reasons = new List<string>();
                using var peers = new HaivenPeerConnections(signaling, _ => transport, () => now);
                peers.Disconnected += (_, reason) => reasons.Add(reason);
                peers.Start(hosting);
                var attemptId = "attempt-1";
                if (!hosting)
                {
                    using var offer = System.Text.Json.JsonDocument.Parse(socket.Sent.Last());
                    attemptId = offer.RootElement.GetProperty("payload").GetProperty("attemptId").GetString()!;
                }
                socket.Deliver(System.Text.Json.JsonSerializer.Serialize(new
                {
                    type = "p2p.signal.received",
                    payload = new
                    {
                        sessionId = "s-1", fromUserId = remote, attemptId, seq = 1,
                        data = System.Text.Json.JsonSerializer.Serialize(new
                        {
                            kind = hosting ? "offer" : "answer", participationId = "p-guest", epoch = 1,
                            part = 1, parts = 1, sdp = "description",
                        }),
                    },
                }));
                peers.Tick();
                socket.Deliver(System.Text.Json.JsonSerializer.Serialize(new
                {
                    type = "p2p.signal.ack",
                    payload = new { sessionId = "s-1", fromUserId = remote, attemptId, seq = 1 },
                }));
                peers.Tick();
                if (connected) transport.Open();
                transport.EmitCandidate();
                check("late candidate awaits its acknowledgement", signaling.PendingAcknowledgements == 1);
                if (refused)
                {
                    using var candidate = System.Text.Json.JsonDocument.Parse(socket.Sent.Last());
                    socket.Deliver(System.Text.Json.JsonSerializer.Serialize(new
                    {
                        type = "p2p.signal", requestId = candidate.RootElement.GetProperty("requestId").GetString(),
                        success = false, error = "invalid_payload",
                    }));
                    peers.Tick();
                }
                else
                    foreach (var time in new[] { 1, 3, 7, 11, 12 }) { now = time; peers.Tick(); }
                var label = $"{(hosting ? "host" : "guest")} {(connected ? "connected" : "pending")} {(refused ? "refusal" : "timeout")}";
                check(label, connected
                    ? !transport.Disposed && reasons.Count == 0 && peers.ConnectedUsers().Contains(remote)
                    : transport.Disposed && reasons.SequenceEqual(new[] { refused ? "invalid_payload" : "signal_timeout" }) && !peers.ConnectedUsers().Any());
                check(label + " clears the failed signal", signaling.PendingAcknowledgements == 0);
            }
        }

        private sealed class Socket : IHaivenSocket
        {
            public event Action<string>? Received;
            public event Action<int, string>? Closed;
            public List<string> Sent { get; } = new List<string>();
            public void Send(string payload) => Sent.Add(payload);
            public void Close() => Closed?.Invoke(1000, "");
            public void Deliver(string payload) => Received?.Invoke(payload);
        }

        private sealed class Signaling : IHaivenPeerSignaling, IHaivenPeerSignalingLifecycle
        {
            public string LocalUserId => "local";
            public bool DiscoverHostOnStart { get; set; }
            public string? HostUserId { get; private set; }
            public bool Start(bool asHost)
            {
                if (!asHost && DiscoverHostOnStart) HostUserId = "host";
                return true;
            }
            public void Poll() { }
            public void Stop() => HostUserId = null;
            public event Action<string>? HostUnreachable { add { } remove { } }
            public List<PeerSignal> Sent { get; } = new List<PeerSignal>();
            public event Action<string, PeerSignal>? Received;
            public void Send(string toUserId, PeerSignal signal) => Sent.Add(signal);
            public void Deliver(string from, PeerSignal signal) => Received?.Invoke(from, signal);
        }
    }

    /// <summary>Scripted WebRTC boundary; real DTLS rejection is covered by the Unity transport tests.</summary>
    sealed class ScriptedPeerTransport : IHaivenPeerTransport
    {
        private readonly TaskCompletionSource<bool> description = new TaskCompletionSource<bool>();
        public bool HoldDescription { get; set; }
        public bool Disposed { get; private set; }
        public string? RemoteSdp { get; private set; }
        public List<PeerIceCandidate> Candidates { get; } = new List<PeerIceCandidate>();
        public event Action<PeerIceCandidate>? LocalCandidate;
        public event Action<PeerChannel>? ChannelOpened;
        public event Action<PeerChannel, byte[]>? Received;
        public event Action<string>? Closed;
        public long RoundTripTimeMs => -1;
        public string? PathType => null;
        public void RefreshRoundTripTime() { }
        public long BufferedAmount(PeerChannel channel) => 0;
        public bool Send(PeerChannel channel, byte[] payload) => !Disposed;

        public async Task<string> CreateOfferAsync()
        {
            if (HoldDescription) await description.Task;
            return "offer";
        }

        public async Task<string> AcceptOfferAsync(string offerSdp)
        {
            RemoteSdp = offerSdp;
            if (HoldDescription) await description.Task;
            return "answer";
        }

        public async Task AcceptAnswerAsync(string answerSdp)
        {
            RemoteSdp = answerSdp;
            if (HoldDescription) await description.Task;
        }

        public void AddRemoteCandidate(PeerIceCandidate candidate) => Candidates.Add(candidate);
        public void CompleteDescription() => description.TrySetResult(true);
        public void EmitCandidate() => LocalCandidate?.Invoke(new PeerIceCandidate("candidate:local", "0", 0));
        public void Receive(byte[] payload) => Received?.Invoke(PeerChannel.Reliable, payload);
        public void Open()
        {
            if (Disposed) return;
            ChannelOpened?.Invoke(PeerChannel.Reliable);
            ChannelOpened?.Invoke(PeerChannel.Unreliable);
        }
        public void Fail() => Closed?.Invoke("fingerprint_mismatch");
        public void Dispose() => Disposed = true;
    }
}
