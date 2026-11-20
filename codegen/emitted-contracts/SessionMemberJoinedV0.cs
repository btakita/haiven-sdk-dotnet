// Generated from the published Haiven contract schemas. Do not edit by hand.
//
// The nullability of a presence discriminant is deliberate and is NOT derived
// from `required`.

#nullable enable

namespace Haiven.Sdk.Contracts
{
	/// <summary>A player joined the peer-to-peer session.</summary>
	public sealed class SessionMemberJoinedV0
	{
		/// <summary>The session.</summary>
		public string SessionId { get; set; } = null!;

		/// <summary>The member.</summary>
		public string UserId { get; set; } = null!;

		/// <summary>The member's participation.</summary>
		public string ParticipationId { get; set; } = null!;
	}
}
