// Generated from the published Haiven contract schemas. Do not edit by hand.
//
// The nullability of a presence discriminant is deliberate and is NOT derived
// from `required`.

#nullable enable

namespace Haiven.Sdk.Contracts
{
	/// <summary>A Google or Apple identity linked to the account from settings.</summary>
	public sealed class AccountIdentityLinkedV0
	{
		/// <summary>The sign-in provider of the identity: google or apple.</summary>
		public string Provider { get; set; } = null!;

		/// <summary>The email address the provider gave for the identity, which can differ from the account's.</summary>
		public string Email { get; set; } = null!;
	}
}
