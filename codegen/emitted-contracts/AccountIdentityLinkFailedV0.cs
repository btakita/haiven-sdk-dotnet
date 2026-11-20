// Generated from the published Haiven contract schemas. Do not edit by hand.
//
// The nullability of a presence discriminant is deliberate and is NOT derived
// from `required`.

#nullable enable

namespace Haiven.Sdk.Contracts
{
	/// <summary>A link of a Google or Apple identity started from settings that was refused or abandoned.</summary>
	public sealed class AccountIdentityLinkFailedV0
	{
		/// <summary>The sign-in provider of the identity: google or apple.</summary>
		public string Provider { get; set; } = null!;

		/// <summary>The error code the link was refused with, such as identity_already_linked.</summary>
		public string Code { get; set; } = null!;
	}
}
