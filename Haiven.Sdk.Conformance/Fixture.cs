using System.Diagnostics;
using System.Text.Json;

namespace Haiven.Sdk.Conformance;

/// <summary>
/// The live fixture, started the same way the node runner starts it.
///
/// The game service's websocket fixture command runs the real websocket stack
/// against a local JWKS endpoint, so a runner needs no docker-compose and no
/// deployed backend, which is what made a second runner cheap enough to write.
///
/// It announces one line of JSON on stdout and then serves. Reading that line
/// rather than polling a fixed port is what lets it bind `:0` and lets several
/// runners exist without agreeing about ports.
/// </summary>
internal sealed class Fixture : IAsyncDisposable
{
	private readonly Process process;
	private readonly string buildDir;
	private readonly HttpClient http = new();

	private Fixture(Process process, string buildDir, Startup info)
	{
		this.process = process;
		this.buildDir = buildDir;
		Info = info;
	}

	internal Startup Info { get; }

	internal sealed record Startup(string BaseUrl, string WsUrl, string UserId);

	/// <summary>Whether this machine can run the fixture at all.</summary>
	internal static string? Unavailable()
	{
		// HAIVEN_API_ROOT names the game service checkout; the conformance make
		// target sets it. A wrong path here does not fail, it skips, which is how
		// stale expectations survive unnoticed.
		var backendRoot = Environment.GetEnvironmentVariable("HAIVEN_API_ROOT");

		if (string.IsNullOrEmpty(backendRoot))
		{
			return "HAIVEN_API_ROOT names no game service checkout";
		}

		if (!File.Exists(Path.Combine(backendRoot, "cmd", "wsfixture", "main.go")))
		{
			return $"no backend checkout at {backendRoot}";
		}

		try
		{
			using var probe = Process.Start(new ProcessStartInfo("go", "version") { RedirectStandardOutput = true })
				?? throw new InvalidOperationException();
			probe.WaitForExit();
			return probe.ExitCode == 0 ? null : "go is not runnable";
		}
		catch (Exception)
		{
			return "no go toolchain";
		}
	}

	internal static async Task<Fixture> StartAsync()
	{
		var backend = Path.GetFullPath(
			Environment.GetEnvironmentVariable("HAIVEN_API_ROOT")
			?? throw new InvalidOperationException("HAIVEN_API_ROOT is not set"));
		var buildDir = Directory.CreateTempSubdirectory("wsfixture-").FullName;
		var binary = Path.Combine(buildDir, "wsfixture");

		using (var build = Process.Start(new ProcessStartInfo("go", $"build -o {binary} ./cmd/wsfixture")
		{
			WorkingDirectory = backend,
			RedirectStandardError = true,
		}) ?? throw new InvalidOperationException("could not start go build"))
		{
			var errors = await build.StandardError.ReadToEndAsync();
			await build.WaitForExitAsync();

			if (build.ExitCode != 0)
			{
				throw new InvalidOperationException($"go build failed: {errors}");
			}
		}

		var process = Process.Start(new ProcessStartInfo(binary, "-addr 127.0.0.1:0")
		{
			RedirectStandardOutput = true,
			RedirectStandardError = true,
		}) ?? throw new InvalidOperationException("could not start wsfixture");

		// A bounded wait, because a fixture that never announces would otherwise
		// hang the whole suite with no output at all.
		var announcement = process.StandardOutput.ReadLineAsync();
		if (await Task.WhenAny(announcement, Task.Delay(TimeSpan.FromSeconds(20))) != announcement)
		{
			process.Kill(entireProcessTree: true);
			throw new TimeoutException("wsfixture did not announce in 20s");
		}

		var line = await announcement
			?? throw new InvalidOperationException($"wsfixture exited: {await process.StandardError.ReadToEndAsync()}");

		using var document = JsonDocument.Parse(line);
		var root = document.RootElement;

		var info = new Startup(
			root.GetProperty("baseUrl").GetString()!,
			root.GetProperty("wsUrl").GetString()!,
			root.GetProperty("userId").GetString()!);

		return new Fixture(process, buildDir, info);
	}

	/// <summary>
	/// A signed access token. `ttl` is a Go duration; a negative one sits inside
	/// the verifier's leeway, so the token verifies while the session it mints is
	/// already lapsed.
	/// </summary>
	internal async Task<string> TokenAsync(string ttl)
	{
		var response = await http.GetStringAsync($"{Info.BaseUrl}/control/token?ttl={Uri.EscapeDataString(ttl)}");
		using var document = JsonDocument.Parse(response);
		return document.RootElement.GetProperty("token").GetString()!;
	}

	/// <summary>Publishes a server-originated frame to this fixture's user.</summary>
	internal async Task PushAsync(string frameJson)
	{
		using var content = new StringContent(frameJson, System.Text.Encoding.UTF8, "application/json");
		var response = await http.PostAsync($"{Info.BaseUrl}/control/push", content);
		response.EnsureSuccessStatusCode();
	}

	public async ValueTask DisposeAsync()
	{
		http.Dispose();

		if (!process.HasExited)
		{
			process.Kill(entireProcessTree: true);
			await process.WaitForExitAsync();
		}

		process.Dispose();

		try
		{
			Directory.Delete(buildDir, recursive: true);
		}
		catch (IOException)
		{
			// A leftover temp directory is not worth failing a suite over.
		}
	}
}
