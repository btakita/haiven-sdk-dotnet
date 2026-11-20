// Generated from the published Haiven contract schemas. Do not edit by hand.
//
// The nullability of a presence discriminant is deliberate and is NOT derived
// from `required`.

#nullable enable

namespace Haiven.Sdk.Contracts
{
	/// <summary>A request to mark a notification category as read.</summary>
	public sealed class NotificationReadRequestV0
	{
		/// <summary>The notification category used for grouping and read state.</summary>
		public string Kind { get; set; } = null!;
	}
}
