// Runner #2 of the conformance suite: dotnet, executing the same corpus the node
// runner executes.
//
// The corpus exists to find out whether the parity scenarios can be written once
// as data and run by four bindings, or whether each needs its own hand-written
// suite. One runner cannot answer that — it only proves the vocabulary is
// expressible in the language it was designed in. The second runner is where the
// question actually gets tested, and anything this one had to special-case is a
// scenario the remaining two will not be able to run either.
//
// Skips loudly rather than passing quietly without `go` or a game service
// checkout (HAIVEN_API_ROOT), like every other live-tier check here.

using System.Text;
using System.Text.Json;

using Haiven.Sdk;
using Haiven.Sdk.Conformance;

// The published gaming suite, named by HAIVEN_GAMING_SUITE (the conformance
// make target sets it), not forked here. The local fork this replaced had
// drifted from the live server.
var corpusPath = Environment.GetEnvironmentVariable("HAIVEN_GAMING_SUITE") ?? "";

if (!File.Exists(corpusPath))
{
	Console.Error.WriteLine($"conformance: no published suite at {corpusPath}");
	return 2;
}

if (Fixture.Unavailable() is { } reason)
{
	Console.Error.WriteLine($"skipping conformance (dotnet): {reason}");
	return 0;
}

using var corpus = JsonDocument.Parse(File.ReadAllText(corpusPath));
var root = corpus.RootElement;

// The token source for THIS runner. It drives the fixture, which mints, so it
// supplies both kinds. A runner driving a real deployment signs in and supplies
// `valid` alone — which is the whole point of the corpus naming a kind rather
// than a ttl.
var supplies = new[] { "valid", "expired" };

var failures = new List<string>();
var ran = 0;
var waived = 0;

await using var fixture = await Fixture.StartAsync();

foreach (var scenario in root.GetProperty("scenarios").EnumerateArray())
{
	var id = scenario.GetProperty("id").GetString()!;

	// A waiver is a recorded gap, not a skip: it names the runner that cannot
	// execute the scenario and why.
	if (scenario.GetProperty("waivers").TryGetProperty("dotnet", out var waiver))
	{
		Console.Error.WriteLine($"waived {id}: {waiver.GetString()}");
		waived++;
		continue;
	}

	// The guard the node runner also carries: reaching a scenario needing a
	// token kind this source cannot produce would otherwise fail deep in the
	// transcript as a protocol error, which reads as a server bug.
	var needed = scenario.GetProperty("steps").EnumerateArray()
		.Where(step => step.GetProperty("do").GetString() == "auth")
		.Select(step => step.TryGetProperty("token", out var kind) ? kind.GetString()! : "valid")
		.ToArray();

	if (needed.FirstOrDefault(kind => !supplies.Contains(kind)) is { } missing)
	{
		failures.Add($"{id}: needs token kind '{missing}' this runner cannot produce; waive it");
		continue;
	}

	try
	{
		await RunScenario(scenario, fixture, failures);
		ran++;
	}
	catch (Exception error)
	{
		failures.Add($"{id}: {error.Message}");
	}
}

if (ran == 0)
{
	failures.Add("no scenarios ran");
}

if (failures.Count == 0)
{
	Console.Error.WriteLine($"ok   conformance (dotnet): {ran} scenario(s), {waived} waived");
	return 0;
}

foreach (var failure in failures)
{
	Console.Error.WriteLine($"FAIL {failure}");
}

return 1;

static async Task RunScenario(JsonElement scenario, Fixture fixture, List<string> failures)
{
	var id = scenario.GetProperty("id").GetString()!;

	SystemNetSocket? socket = null;
	var inbox = new Queue<JsonElement>();
	var documents = new List<JsonDocument>();
	var gate = new SemaphoreSlim(0);

	try
	{
		foreach (var step in scenario.GetProperty("steps").EnumerateArray())
		{
			switch (step.GetProperty("do").GetString())
			{
				case "connect":
					socket = new SystemNetSocket();
					socket.Received += payload =>
					{
						var document = JsonDocument.Parse(payload);
						lock (documents)
						{
							documents.Add(document);
							inbox.Enqueue(document.RootElement);
						}

						gate.Release();
					};
					await socket.ConnectAsync(new Uri(fixture.Info.WsUrl));
					break;

				case "auth":
				{
					Require(socket, id, "auth before connect");
					var kind = step.TryGetProperty("token", out var k) ? k.GetString()! : "valid";
					var token = await fixture.TokenAsync(kind == "expired" ? "-1s" : "1h");
					var requestId = step.TryGetProperty("requestId", out var r) ? r.GetString()! : string.Empty;
					socket!.Send($"{{\"type\":\"auth\",\"requestId\":{JsonSerializer.Serialize(requestId)},\"payload\":{JsonSerializer.Serialize(token)}}}");
					break;
				}

				case "send":
					Require(socket, id, "send before connect");
					socket!.Send(step.GetProperty("frame").GetRawText());
					break;

				case "push":
					await fixture.PushAsync(step.GetProperty("frame").GetRawText());
					break;

				case "expect":
				{
					if (await Next(gate, inbox, TimeSpan.FromSeconds(5)) is not { } frame)
					{
						throw new TimeoutException($"expected {step.GetProperty("type").GetString()}, nothing arrived");
					}

					Expect(id, step, frame, failures);
					break;
				}

				case "expectSilence":
				{
					var window = TimeSpan.FromMilliseconds(step.GetProperty("ms").GetInt32());
					if (await Next(gate, inbox, window) is { } stray)
					{
						failures.Add($"{id}: expected silence, got {stray.GetProperty("type").GetString()}");
					}

					break;
				}

				default:
					failures.Add($"{id}: unknown step '{step.GetProperty("do").GetString()}'");
					break;
			}
		}
	}
	finally
	{
		socket?.Dispose();
		foreach (var document in documents) document.Dispose();
		gate.Dispose();
	}
}

static void Require(SystemNetSocket? socket, string id, string what)
{
	if (socket is null) throw new InvalidOperationException($"{id}: {what}");
}

static async Task<JsonElement?> Next(SemaphoreSlim gate, Queue<JsonElement> inbox, TimeSpan within)
{
	if (!await gate.WaitAsync(within)) return null;

	lock (inbox)
	{
		return inbox.Dequeue();
	}
}

/// <summary>`expect`: `type` is required, `keys` pins the exact key set, `fields` pins values.</summary>
static void Expect(string id, JsonElement step, JsonElement frame, List<string> failures)
{
	var expectedType = step.GetProperty("type").GetString();
	var actualType = frame.TryGetProperty("type", out var t) ? t.GetString() : null;

	if (expectedType != actualType)
	{
		failures.Add($"{id}: expected type {expectedType}, got {actualType}");
		return;
	}

	if (step.TryGetProperty("keys", out var keys))
	{
		var expected = keys.EnumerateArray().Select(k => k.GetString()!).OrderBy(k => k, StringComparer.Ordinal);
		var actual = frame.EnumerateObject().Select(p => p.Name).OrderBy(k => k, StringComparer.Ordinal);

		if (!expected.SequenceEqual(actual))
		{
			failures.Add($"{id}: key set is [{string.Join(", ", actual)}], expected [{string.Join(", ", expected)}]");
		}
	}

	if (!step.TryGetProperty("fields", out var fields)) return;

	foreach (var field in fields.EnumerateObject())
	{
		if (!frame.TryGetProperty(field.Name, out var actual))
		{
			// Presence, not value. This is the assertion that catches the F10
			// collapse from the corpus side: a binding that dropped `success`
			// would fail here rather than silently misclassifying.
			failures.Add($"{id}: field '{field.Name}' is absent");
			continue;
		}

		if (actual.GetRawText() != field.Value.GetRawText())
		{
			failures.Add($"{id}: field '{field.Name}' is {actual.GetRawText()}, expected {field.Value.GetRawText()}");
		}
	}
}
