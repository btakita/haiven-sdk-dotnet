// Generated from the published Haiven contract schemas. Do not edit by hand.
//
// The nullability of a presence discriminant is deliberate and is NOT derived
// from `required`.

#nullable enable

namespace Haiven.Sdk.Contracts
{
	/// <summary>A request to replace a chat message's content.</summary>
	public sealed class ChatEditRequestV0
	{
		/// <summary>The chat message identifier.</summary>
		public string MessageId { get; set; } = null!;

		/// <summary>The user-authored message content.</summary>
		public string Content { get; set; } = null!;
	}
}
