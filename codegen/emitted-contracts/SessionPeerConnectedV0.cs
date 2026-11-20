// Generated from the published Haiven contract schemas. Do not edit by hand.
//
// The nullability of a presence discriminant is deliberate and is NOT derived
// from `required`.

#nullable enable

namespace Haiven.Sdk.Contracts
{
	/// <summary>The host's report that the data channel to a guest is open.</summary>
	public sealed class SessionPeerConnectedV0
	{
		/// <summary>The session the guest belongs to.</summary>
		public string SessionId { get; set; } = null!;

		/// <summary>The guest whose data channel opened.</summary>
		public string UserId { get; set; } = null!;
	}
}
