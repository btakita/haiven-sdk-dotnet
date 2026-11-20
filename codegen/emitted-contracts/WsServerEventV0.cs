// Generated from the published Haiven contract schemas. Do not edit by hand.
//
// The nullability of a presence discriminant is deliberate and is NOT derived
// from `required`.

#nullable enable

namespace Haiven.Sdk.Contracts
{
	/// <summary>A server frame the client did not ask for.</summary>
	public sealed class WsServerEventV0
	{
		/// <summary>The frame discriminator, naming what happened.</summary>
		public string Type { get; set; } = null!;

		/// <summary>The event body, whose shape is determined by the type field.</summary>
		public object? Payload { get; set; }
	}
}
